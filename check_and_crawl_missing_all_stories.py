import asyncio
from aiogram import Bot, Dispatcher
from core.crawler_missing_chapter import loop_once_multi_sites
from utils.logger import logger
from utils.telegram_options import telegram_router
from config.config import  TELEGRAM_BOT_TOKEN


def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--force-unskip', action='store_true', help='Bỏ qua skip_crawl để crawl lại toàn bộ truyện skip')
    parser.add_argument('--telegram', action='store_true', help='Khởi động Telegram bot mode')   # <-- THÊM DÒNG NÀY
    return parser.parse_args()

async def run_telegram_bot():
    bot = Bot(token=TELEGRAM_BOT_TOKEN) #type:ignore
    dp = Dispatcher()
    dp.include_router(telegram_router)
    logger.info("[Telegram Bot] Đang chờ lệnh /startcrawl từ Telegram...")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    args = parse_args()
    if args.telegram:
        import asyncio
        asyncio.run(run_telegram_bot())
    else:
        asyncio.run(loop_once_multi_sites(force_unskip=args.force_unskip))

