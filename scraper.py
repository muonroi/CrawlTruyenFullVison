import time
import requests
import cloudscraper
from typing import Optional, Dict
from utils.utils import logger
from config.config import REQUEST_DELAY, DEFAULT_HEADERS, BASE_URL
from config.proxy import get_next_proxy_settings

scraper: Optional[cloudscraper.CloudScraper] = None

def initialize_scraper(override_headers: Optional[Dict[str, str]] = None) -> None:
    """
    Khởi tạo cloudscraper instance.
    """
    global scraper
    try:
        scraper = cloudscraper.create_scraper()
        current_headers = DEFAULT_HEADERS.copy()
        if override_headers:
            current_headers.update(override_headers)
        if BASE_URL:
            current_headers["Referer"] = BASE_URL + "/"
        else:
            current_headers.pop("Referer", None)


        scraper.headers.update(current_headers)
        logger.info("Cloudscraper initialized.")
    except Exception as e:
        logger.error(f"Lỗi khi khởi tạo Cloudscraper: {e}")
        scraper = None # Đảm bảo scraper là None nếu khởi tạo lỗi


def make_request(url, headers_override=None, retries=3, delay_on_retry=5):
    """
    Thực hiện request HTTP với cloudscraper, xử lý retry, proxy và headers.
    """
    global scraper
    if scraper is None:
        initialize_scraper()  # Đảm bảo scraper đã được khởi tạo

    current_headers = scraper.headers.copy()  # Lấy headers mặc định từ scraper
    if headers_override:
        current_headers.update(headers_override)

    for attempt in range(retries):
        proxy_settings = get_next_proxy_settings()  # Lấy proxy cho lần thử này
        proxy_to_log = (
            proxy_settings["http"]
            if proxy_settings and "http" in proxy_settings
            else "Không có"
        )

        try:
            print(f"Requesting: {url} (Lần {attempt + 1}) với proxy: {proxy_to_log}")
            time.sleep(REQUEST_DELAY)
            response = scraper.get(
                url, headers=current_headers, proxies=proxy_settings, timeout=30
            )  # Tăng timeout một chút
            response.raise_for_status()
            response.encoding = (
                response.apparent_encoding
            )  # Quan trọng để xử lý đúng encoding
            return response
        except requests.exceptions.HTTPError as http_err:
            print(
                f"Lỗi HTTP: {http_err} khi truy cập {url} (proxy: {proxy_to_log}, lần {attempt + 1}/{retries})"
            )
            if hasattr(http_err, "response") and http_err.response is not None:
                if http_err.response.status_code == 404:
                    print(f"Lỗi 404 tại {url}, không thử lại.")
                    return None
                # Cloudflare có thể trả về 403 hoặc các mã lỗi khác khi challenge thất bại hoặc IP bị block
                if http_err.response.status_code == 403:
                    print(
                        f"Lỗi 403 (Forbidden) tại {url}. Có thể Cloudflare đã chặn proxy/IP hoặc challenge thất bại."
                    )
                # Các lỗi 5xx có thể do server quá tải hoặc Cloudflare đang gặp vấn đề
                if str(http_err.response.status_code).startswith("5"):
                    print(
                        f"Lỗi Server ({http_err.response.status_code}) tại {url}. Có thể thử lại với proxy khác."
                    )

        except requests.exceptions.ProxyError as proxy_err:
            print(
                f"Lỗi Proxy: {proxy_err} khi truy cập {url} với proxy {proxy_to_log} (lần {attempt + 1}/{retries})"
            )
            # Nếu lỗi proxy, không cần chờ delay_on_retry lâu, thử proxy khác ngay trong lần retry tiếp theo.
            # Nhưng vẫn nên có một chút delay để tránh spam.
            time.sleep(REQUEST_DELAY * 2)
            continue  # Chuyển sang lần thử tiếp theo (có thể với proxy khác)

        except requests.exceptions.ConnectionError as conn_err:
            print(
                f"Lỗi kết nối: {conn_err} khi truy cập {url} (proxy: {proxy_to_log}, lần {attempt + 1}/{retries})"
            )
        except requests.exceptions.Timeout as timeout_err:
            print(
                f"Lỗi timeout: {timeout_err} khi truy cập {url} (proxy: {proxy_to_log}, lần {attempt + 1}/{retries})"
            )
        except cloudscraper.exceptions.CloudflareChallengeError as cf_challenge_err:
            # cloudscraper không giải quyết được challenge
            print(
                f"Lỗi Cloudflare Challenge: {cf_challenge_err} tại {url} (proxy: {proxy_to_log}, lần {attempt + 1}/{retries})"
            )
            print("Thử đổi proxy hoặc kiểm tra cấu hình Cloudflare của trang web.")
        except requests.exceptions.RequestException as req_err:
            print(
                f"Lỗi request chung: {req_err} khi truy cập {url} (proxy: {proxy_to_log}, lần {attempt + 1}/{retries})"
            )

        if attempt < retries - 1:
            print(f"Đang thử lại sau {delay_on_retry} giây...")
            time.sleep(delay_on_retry)
        else:
            print(f"Không thể truy cập {url} sau {retries} lần thử.")
    return None