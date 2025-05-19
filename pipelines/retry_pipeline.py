
import json
import random
import time
import aiohttp
import os
from pipelines.genre_pipeline import process_genre_with_limit
from utils.batch_utils import smart_delay
from utils.logger import logger
from config.config import FAILED_GENRES_FILE, RETRY_GENRE_ROUND_LIMIT, RETRY_SLEEP_SECONDS, get_state_file
from config.proxy_provider import shuffle_proxies
from utils.notifier import send_telegram_notify
from core.models.story import Genre


async def retry_failed_genres(adapter, site_key):
    round_idx = 0
    while True:
        if not os.path.exists(FAILED_GENRES_FILE):
            break
        with open(FAILED_GENRES_FILE, "r", encoding="utf-8") as f:
            failed_genres_raw = json.load(f)
        if not failed_genres_raw:
            break

        # Chuyển dict sang object Genre
        failed_genres = []
        for g in failed_genres_raw:
            try:
                failed_genres.append(Genre(**g))
            except Exception as ex:
                logger.warning(f"[Retry] Parse genre fail: {g} - {ex}")

        round_idx += 1
        logger.warning(f"=== [RETRY ROUND {round_idx}] Đang retry {len(failed_genres)} thể loại bị fail... ===")
        await send_telegram_notify(f"[Crawler] Retry round {round_idx}: còn {len(failed_genres)} thể loại fail")

        to_remove = []
        async with aiohttp.ClientSession() as session:
            random.shuffle(failed_genres)
            for genre in failed_genres:
                delay = min(60, 5 * (2 ** getattr(genre, "fail_count", 1)))  # Tối đa 1 phút delay/gen
                await smart_delay(delay)
                try:
                    await process_genre_with_limit(session, genre, {}, adapter, site_key)
                    # Nếu không lỗi thì xóa khỏi fail
                    to_remove.append(genre)
                    logger.info(f"[RETRY] Thành công genre: {genre.name}")
                except Exception as ex:
                    # Nếu muốn lưu fail_count, nên convert object sang dict rồi update
                    setattr(genre, "fail_count", getattr(genre, "fail_count", 1) + 1)
                    logger.error(f"[RETRY] Vẫn lỗi genre: {genre.name}: {ex}")

        # Update lại file failed_genres.json
        if to_remove:
            # Chuyển lại list object thành dict để ghi file
            failed_genres = [g for g in failed_genres if g not in to_remove]
            with open(FAILED_GENRES_FILE, "w", encoding="utf-8") as f:
                json.dump([g.dict() for g in failed_genres], f, ensure_ascii=False, indent=4)

        if failed_genres:
            if round_idx < RETRY_GENRE_ROUND_LIMIT:
                shuffle_proxies()
                logger.warning(f"Còn {len(failed_genres)} genre fail, bắt đầu vòng retry tiếp theo...")
                continue
            else:
                shuffle_proxies()
                genre_names = ", ".join([g.name for g in failed_genres])
                await send_telegram_notify(
                    f"[Crawler] Sau {RETRY_GENRE_ROUND_LIMIT} vòng retry, còn {len(failed_genres)} genre lỗi: {genre_names}. Sẽ sleep {RETRY_SLEEP_SECONDS//60} phút trước khi thử lại."
                )
                logger.error(f"Sleep {RETRY_SLEEP_SECONDS//60} phút rồi retry lại các genre fail: {genre_names}")
                time.sleep(RETRY_SLEEP_SECONDS)
                round_idx = 0
                continue
        else:
            logger.info("Tất cả genre fail đã retry thành công.")
            break



