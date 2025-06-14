import aiohttp

from config.config import  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
async def send_telegram_notify(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=payload) as resp:
            return await resp.json()
        
async def notify_genre_completed(genre_name):
    message = f"Tất cả truyện của thể loại '{genre_name}' đã crawl xong!"
    await send_telegram_notify(message)


async def send_retry_queue_report(queue_file="chapter_retry_queue.json"):
    """Gửi báo cáo tổng hợp số chương lỗi trong queue."""
    import json, os
    if not os.path.exists(queue_file):
        return
    try:
        with open(queue_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return
    if not data:
        return
    counts = {}
    for item in data:
        site = item.get("site") or "unknown"
        counts[site] = counts.get(site, 0) + 1
    lines = [f"{k}: {v}" for k, v in counts.items()]
    msg = "[Queue Report] Chương lỗi còn lại: " + ", ".join(lines)
    await send_telegram_notify(msg)
