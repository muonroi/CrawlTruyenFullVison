import os
import asyncio
from typing import List, Optional, Dict
import aiofiles
from config.config import PROXIES_FILE, GLOBAL_PROXY_USERNAME, GLOBAL_PROXY_PASSWORD

# Proxy list and pointer
loaded_proxies: List[str] = []
current_proxy_index: int = 0

async def load_proxies(filename: str = PROXIES_FILE) -> List[str]:
    """
    Bất đồng bộ: đọc file proxy, lưu vào loaded_proxies.
    """
    global loaded_proxies
    loaded_proxies = []
    loop = asyncio.get_event_loop()
    exists = await loop.run_in_executor(None, os.path.exists, filename)
    if not exists:
        print(f"File proxy {filename} không tồn tại. Sẽ không sử dụng proxy.")
        return loaded_proxies

    try:
        async with aiofiles.open(filename, mode="r") as f:
            lines = await f.readlines()
        raw_entries = [line.strip() for line in lines if line.strip() and not line.startswith("#")]
        if not raw_entries:
            print(f"File proxy {filename} rỗng hoặc chỉ chứa comment. Không có proxy nào được tải.")
            return loaded_proxies

        loaded_proxies = raw_entries
        print(f"Đã tải {len(loaded_proxies)} mục proxy thô từ {filename}.")
        if loaded_proxies:
            print(f"  Ví dụ mục proxy đầu tiên: '{loaded_proxies[0]}'")
            if GLOBAL_PROXY_USERNAME and GLOBAL_PROXY_PASSWORD:
                print(f"  Các mục IP:PORT sẽ được xác thực với user: '{GLOBAL_PROXY_USERNAME}'.")
            else:
                print("  CẢNH BÁO: GLOBAL_PROXY_USERNAME hoặc GLOBAL_PROXY_PASSWORD chưa được đặt! Proxy IP:PORT sẽ không xác thực.")
    except Exception as e:
        print(f"LỖI khi đọc file proxy {filename}: {e}")
    return loaded_proxies

async def get_next_proxy_settings() -> Optional[Dict[str, str]]:
    """
    Bất đồng bộ: lấy proxy kế tiếp, định dạng URL đầy đủ.
    Trả về dict {'http': url, 'https': url} hoặc None.
    """
    global loaded_proxies, current_proxy_index
    if not loaded_proxies:
        return None

    raw_proxy = loaded_proxies[current_proxy_index]
    current_proxy_index = (current_proxy_index + 1) % len(loaded_proxies)

    if "://" in raw_proxy:
        final_url = raw_proxy
    else:
        # IP:PORT
        if GLOBAL_PROXY_USERNAME and GLOBAL_PROXY_PASSWORD:
            final_url = f"http://{GLOBAL_PROXY_USERNAME}:{GLOBAL_PROXY_PASSWORD}@{raw_proxy}"
        else:
            final_url = f"http://{raw_proxy}"
            if ":" in raw_proxy:
                print(f"Cảnh báo: Proxy '{raw_proxy}' không có xác thực global. Nếu cần auth, kết nối có thể thất bại.")

    return {"http": final_url, "https": final_url}