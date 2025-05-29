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
