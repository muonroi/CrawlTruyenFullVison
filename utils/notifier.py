import importlib
import importlib.util
from typing import Any, Dict

from config.config import DISCORD_WEBHOOK_URL

_AIOHTTP_SPEC = importlib.util.find_spec("aiohttp")
if _AIOHTTP_SPEC:
    aiohttp = importlib.import_module("aiohttp")  # type: ignore
else:
    aiohttp = None

async def send_discord_notify(message: str):
    """Gửi thông báo đến Discord webhook."""
    if not DISCORD_WEBHOOK_URL:
        return
    payload: Dict[str, Any] = {"content": message}
    if aiohttp is None:
        print(f"[Discord Notify] aiohttp not available. Message: {message}")
        return
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10) as resp:
                return await resp.json()
    except Exception as ex:
        print(f"[Discord Notify] Gửi lỗi: {ex}")

async def notify_genre_completed(genre_name):
    message = f"Tất cả truyện của thể loại '{genre_name}' đã crawl xong!"
    await send_discord_notify(message)


