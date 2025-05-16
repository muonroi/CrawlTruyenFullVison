# proxies.py
import os
from typing import List
from config.config import PROXIES_FILE, GLOBAL_PROXY_USERNAME, GLOBAL_PROXY_PASSWORD
# --- Biến toàn cục cho proxy ---
loaded_proxies: List[str] = []
current_proxy_index: int = 0

def load_proxies(filename=PROXIES_FILE):
    """
    Tải danh sách proxy từ file.
    Mỗi dòng trong file được kỳ vọng là:
    - IP:PORT (ví dụ: 43.153.73.54:19405) - sẽ được xác thực bằng GLOBAL_PROXY_USERNAME/PASSWORD.
    - Hoặc một URL proxy đầy đủ (ví dụ: http://userkhac:passkhac@hostkhac:port) nếu bạn muốn ghi đè global credentials cho một số proxy cụ thể.
    """
    global loaded_proxies
    raw_entries = []  # Sẽ chứa các dòng đọc được từ file

    if not os.path.exists(filename):
        print(f"File proxy {filename} không tồn tại. Sẽ không sử dụng proxy.")
        loaded_proxies = []
        return loaded_proxies

    try:
        with open(filename, "r") as f:
            raw_entries = [
                line.strip() for line in f if line.strip() and not line.startswith("#")
            ]

        if not raw_entries:
            print(
                f"File proxy {filename} rỗng hoặc chỉ chứa comment. Không có proxy nào được tải."
            )
            loaded_proxies = []
            return loaded_proxies

        loaded_proxies = raw_entries  # Lưu trữ các mục thô, get_next_proxy_settings sẽ định dạng chúng
        print(f"Đã tải {len(loaded_proxies)} mục proxy thô từ {filename}.")
        if loaded_proxies:
            print(f"  Ví dụ mục proxy thô đầu tiên: '{loaded_proxies[0]}'")
            if GLOBAL_PROXY_USERNAME and GLOBAL_PROXY_PASSWORD:
                print(
                    f"  Các mục dạng IP:PORT sẽ được thử xác thực với user: '{GLOBAL_PROXY_USERNAME}'."
                )
            else:
                print(
                    "  CẢNH BÁO: GLOBAL_PROXY_USERNAME hoặc GLOBAL_PROXY_PASSWORD chưa được đặt trong code! Các proxy IP:PORT sẽ được dùng không có xác thực."
                )

    except IOError as e:
        print(f"LỖI khi đọc file proxy {filename}: {e}")
        loaded_proxies = []

    return loaded_proxies


def get_next_proxy_settings():
    """
    Lấy proxy tiếp theo từ danh sách đã tải và định dạng nó thành URL đầy đủ.
    - Nếu mục trong file là 'IP:PORT', nó sẽ sử dụng GLOBAL_PROXY_USERNAME/PASSWORD (nếu được đặt trong code) để tạo URL xác thực.
    - Nếu mục trong file đã là một URL đầy đủ (ví dụ: http://user:pass@host:port), nó sẽ được sử dụng trực tiếp.
    Trả về dict proxy cho thư viện requests hoặc None nếu không có proxy.
    """
    global loaded_proxies, current_proxy_index
    if not loaded_proxies:
        return None

    raw_proxy_entry = loaded_proxies[
        current_proxy_index
    ]  # Ví dụ: "43.153.73.54:19405" hoặc "http://user:pass@khac.com:123"
    current_proxy_index = (current_proxy_index + 1) % len(loaded_proxies)

    final_proxy_url = None

    if "://" in raw_proxy_entry:
        # Mục này đã là một URL đầy đủ, có thể đã bao gồm user:pass khác
        # Ví dụ: người dùng tự nhập http://otheruser:otherpass@differenthost:port vào proxies.txt
        final_proxy_url = raw_proxy_entry
        # print(f"Debug: Sử dụng mục proxy đã có scheme từ file: {final_proxy_url}")
    else:
        # Mục này có dạng IP:PORT hoặc HOST:PORT (ví dụ: "43.153.73.54:19405")
        # Sẽ áp dụng GLOBAL_PROXY_USERNAME và GLOBAL_PROXY_PASSWORD nếu chúng được đặt
        host_port_str = raw_proxy_entry  # Đây chính là "IP:PORT"
        if GLOBAL_PROXY_USERNAME and GLOBAL_PROXY_PASSWORD:
            final_proxy_url = f"http://{GLOBAL_PROXY_USERNAME}:{GLOBAL_PROXY_PASSWORD}@{host_port_str}"
            # print(f"Debug: Định dạng proxy '{host_port_str}' với global credentials thành: {final_proxy_url}")
        else:
            # Không có global credentials, chỉ thêm scheme http (proxy không xác thực)
            final_proxy_url = f"http://{host_port_str}"
            # print(f"Debug: Định dạng proxy '{host_port_str}' không có global credentials thành: {final_proxy_url}")
            if ":" in host_port_str:  # Chỉ hiện cảnh báo nếu nó thực sự giống IP:PORT
                print(
                    f"  Cảnh báo: Proxy '{host_port_str}' đang được sử dụng mà không có GLOBAL credentials (chưa đặt trong code). "
                    f"Nếu proxy này yêu cầu xác thực, kết nối có thể thất bại."
                )

    if final_proxy_url:
        return {
            "http": final_proxy_url,
            "https": final_proxy_url,  # Rất quan trọng cho HTTPS tunnel
        }
    else:
        # Điều này không nên xảy ra nếu loaded_proxies không rỗng và logic ở trên đúng
        print(f"Lỗi nghiêm trọng: Không thể định dạng proxy entry: '{raw_proxy_entry}'")
        return None