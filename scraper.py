import time
import requests
import cloudscraper
from typing import Optional, Dict # Thêm Any nếu cần cho các kiểu dữ liệu phức tạp hơn
from cloudscraper.exceptions import CloudflareChallengeError # Import từ submodule exceptions

from requests.exceptions import RequestException, HTTPError, ProxyError, ConnectionError, Timeout as RequestsTimeout

# Đảm bảo đường dẫn import là chính xác dựa trên cấu trúc thư mục của bạn
from utils.utils import logger # Sử dụng logger đã thiết lập
from config.config import (
    REQUEST_DELAY,
    get_random_headers, # Hàm lấy header ngẫu nhiên
    TIMEOUT_REQUEST,    # Thời gian timeout từ config
    RETRY_ATTEMPTS,     # Số lần thử lại từ config
    DELAY_ON_RETRY      # Độ trễ khi thử lại từ config
)
from config.proxy import get_next_proxy_settings # Đổi tên module proxy cho đúng

# Biến toàn cục cho cloudscraper instance
scraper: Optional[cloudscraper.CloudScraper] = None

def initialize_scraper(override_headers: Optional[Dict[str, str]] = None) -> None:
    """
    Khởi tạo cloudscraper instance với headers ngẫu nhiên.
    """
    global scraper
    try:
        # Tạo scraper instance. Có thể thêm các tùy chọn cho create_scraper nếu cần.
        # Ví dụ: scraper = cloudscraper.create_scraper(delay=10, browser={'custom': 'ScraperBot/1.0'})
        scraper = cloudscraper.create_scraper()

        # Lấy headers ngẫu nhiên từ hàm đã định nghĩa (trong config.py hoặc ở đây)
        current_headers = get_random_headers()

        # Ghi đè hoặc bổ sung headers nếu có override_headers
        if override_headers:
            current_headers.update(override_headers)

        # Logic đặt Referer đã được chuyển vào get_random_headers() trong config.py
        # Nếu bạn muốn logic Referer ở đây, bạn có thể làm như sau:
        # if BASE_URL and "Referer" not in current_headers: # Chỉ đặt nếu chưa có và BASE_URL tồn tại
        #     current_headers["Referer"] = BASE_URL + ("/" if not BASE_URL.endswith("/") else "")
        # elif "Referer" not in current_headers: # Nếu không có BASE_URL và chưa có Referer
        #     current_headers.pop("Referer", None)


        # Cập nhật headers cho scraper session
        scraper.headers.update(current_headers)
        logger.info(f"Cloudscraper initialized with User-Agent: {current_headers.get('User-Agent')}")
        logger.debug(f"Full headers for session: {scraper.headers}")

    except Exception as e:
        logger.error(f"Lỗi khi khởi tạo Cloudscraper: {e}")
        scraper = None # Đảm bảo scraper là None nếu khởi tạo lỗi


def make_request(url: str,
                 headers_override: Optional[Dict[str, str]] = None,
                 retries: int = RETRY_ATTEMPTS, # Sử dụng hằng số từ config
                 delay_on_retry: int = DELAY_ON_RETRY, # Sử dụng hằng số từ config
                 request_delay_override: Optional[float] = None
                ) -> Optional[requests.Response]:
    """
    Thực hiện request HTTP với cloudscraper, xử lý retry, proxy và headers.
    Sử dụng User-Agent ngẫu nhiên được thiết lập khi scraper được khởi tạo.
    """
    global scraper
    if scraper is None:
        logger.warning("Scraper chưa được khởi tạo. Thử khởi tạo lại...")
        initialize_scraper() # initialize_scraper sẽ đặt User-Agent ngẫu nhiên cho session
        if scraper is None: # Nếu vẫn lỗi sau khi thử khởi tạo lại
            logger.error("Không thể khởi tạo scraper. Request thất bại.")
            return None

    # Lấy headers từ session của scraper (đã có User-Agent ngẫu nhiên)
    # Không cần gọi get_random_headers() ở đây nữa nếu bạn muốn User-Agent nhất quán trong một session
    current_session_headers = scraper.headers.copy()

    # Nếu có headers_override cụ thể cho request này, áp dụng chúng
    # Điều này cho phép ghi đè User-Agent của session nếu cần thiết cho một request cụ thể
    if headers_override:
        current_session_headers.update(headers_override)
        logger.debug(f"Requesting {url} with overridden headers: {current_session_headers}")


    effective_request_delay = request_delay_override if request_delay_override is not None else REQUEST_DELAY

    for attempt in range(retries):
        proxy_settings = get_next_proxy_settings()
        proxy_to_log = (
            proxy_settings["http"]
            if proxy_settings and "http" in proxy_settings
            else "Không có"
        )

        try:
            logger.info(f"Requesting: {url} (Lần {attempt + 1}/{retries}) với proxy: {proxy_to_log}")
            if attempt > 0 or effective_request_delay > 0: # Chỉ sleep nếu là retry hoặc có delay
                 time.sleep(effective_request_delay)

            response = scraper.get(
                url,
                headers=current_session_headers, # Sử dụng headers đã chuẩn bị
                proxies=proxy_settings,
                timeout=TIMEOUT_REQUEST # Sử dụng hằng số từ config
            )
            response.raise_for_status() # Ném HTTPError cho các mã lỗi 4xx/5xx

            # Xử lý encoding một cách cẩn thận hơn
            if response.encoding is None or response.encoding.lower() == 'iso-8859-1':
                # ISO-8859-1 thường là dấu hiệu của việc encoding không được xác định đúng
                # response.apparent_encoding có thể tốt hơn trong trường hợp này
                response.encoding = response.apparent_encoding
                logger.debug(f"Encoding không xác định hoặc ISO-8859-1, đổi thành apparent_encoding: {response.encoding} cho URL: {url}")
            elif 'charset' not in response.headers.get('content-type', '').lower():
                # Nếu header Content-Type không chỉ định charset, apparent_encoding có thể chính xác hơn
                apparent_enc = response.apparent_encoding
                if response.encoding.lower() != apparent_enc.lower():
                    logger.debug(f"Content-Type không có charset, apparent_encoding ({apparent_enc}) khác với encoding hiện tại ({response.encoding}). Cân nhắc dùng apparent_encoding nếu có vấn đề.")

            logger.debug(f"Request thành công {url} với status {response.status_code}, encoding: {response.encoding}")
            return response

        except HTTPError as http_err:
            status_code_str = str(http_err.response.status_code) if hasattr(http_err, "response") and http_err.response is not None else "N/A"
            logger.warning(
                f"Lỗi HTTP {status_code_str}: {http_err} khi truy cập {url} (proxy: {proxy_to_log}, lần {attempt + 1}/{retries})"
            )
            if hasattr(http_err, "response") and http_err.response is not None:
                if http_err.response.status_code == 404:
                    logger.error(f"Lỗi 404 tại {url}, không thử lại.")
                    return None # Trả về None cho 404
                if http_err.response.status_code == 403:
                    logger.warning(
                        f"Lỗi 403 (Forbidden) tại {url}. Có thể Cloudflare đã chặn proxy/IP hoặc challenge thất bại. User-Agent: {current_session_headers.get('User-Agent')}"
                    )
                if status_code_str.startswith("5"): # Lỗi server
                    logger.warning(
                        f"Lỗi Server ({status_code_str}) tại {url}. Có thể thử lại với proxy khác."
                    )
        except ProxyError as proxy_err:
            logger.warning(
                f"Lỗi Proxy: {proxy_err} khi truy cập {url} với proxy {proxy_to_log} (lần {attempt + 1}/{retries})"
            )
            # Khi lỗi proxy, thường nên thử proxy khác ngay, không cần chờ delay_on_retry quá lâu
            # nhưng vẫn nên có một chút delay để tránh spam request nếu tất cả proxy đều lỗi
            if attempt < retries - 1: time.sleep(effective_request_delay if effective_request_delay > 1 else 1) # Chờ ngắn
            continue # Chuyển sang lần thử tiếp theo (sẽ lấy proxy mới)

        except ConnectionError as conn_err: # Bao gồm DNS lookup, refused connection,...
            logger.warning(
                f"Lỗi kết nối: {conn_err} khi truy cập {url} (proxy: {proxy_to_log}, lần {attempt + 1}/{retries})"
            )
        except RequestsTimeout as timeout_err: # Phân biệt Timeout của thư viện requests
            logger.warning(
                f"Lỗi timeout (requests): {timeout_err} khi truy cập {url} (proxy: {proxy_to_log}, lần {attempt + 1}/{retries})"
            )
        except CloudflareChallengeError as cf_challenge_err:
            # cloudscraper không giải quyết được challenge của Cloudflare
            logger.error(
                f"Lỗi Cloudflare Challenge: {cf_challenge_err} tại {url} (proxy: {proxy_to_log}, lần {attempt + 1}/{retries}). User-Agent: {current_session_headers.get('User-Agent')}"
            )
            logger.info("Thử đổi proxy, kiểm tra cấu hình Cloudflare của trang web, hoặc User-Agent có thể không phù hợp.")
            # Có thể bạn muốn dừng lại ở đây nếu gặp lỗi này liên tục, hoặc thử khởi tạo lại scraper với cài đặt khác.
        except RequestException as req_err: # Bắt các lỗi request chung khác từ thư viện requests
            logger.error(
                f"Lỗi request chung: {req_err} khi truy cập {url} (proxy: {proxy_to_log}, lần {attempt + 1}/{retries})"
            )
        except Exception as e: # Bắt các lỗi không mong muốn khác
             logger.error(
                f"Lỗi không xác định trong make_request khi truy cập {url}: {e} (proxy: {proxy_to_log}, lần {attempt + 1}/{retries})"
            )


        if attempt < retries - 1:
            logger.info(f"Đang thử lại sau {delay_on_retry} giây...")
            time.sleep(delay_on_retry)
        else:
            logger.error(f"Không thể truy cập {url} sau {retries} lần thử.")
    return None
