import asyncio
import json
import os
from adapters.factory import get_adapter
from config.config import ASYNC_SEMAPHORE_LIMIT
from utils.logger import logger
from utils.chapter_utils import extract_real_chapter_number, get_chapter_filename


SEM = asyncio.Semaphore(ASYNC_SEMAPHORE_LIMIT)



async def sync_chapter_with_yy_first(story_folder, metadata):
    yy_source = None
    for src in metadata.get("sources", []):
        if isinstance(src, dict) and (src.get("site_key") == "truyenyy" or src.get("site") == "truyenyy"):
            yy_source = src
            break

    # Nếu chưa có, tự động sinh url YY và check tồn tại
    if not yy_source:
        # Sinh slug
        from utils.chapter_utils import slugify_title
        title = metadata.get("title", "")
        slug = slugify_title(title)
        yy_url = f"https://truyenyy.co/truyen/{slug}/"
        # Check xem truyện có thật không
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get(yy_url) as resp:
                text = await resp.text()
                if resp.status == 200 and ("class=\"story-detail\"" in text or "/danh-sach-chuong" in text):
                    yy_source = {"url": yy_url, "site_key": "truyenyy"}
                    metadata.setdefault("sources", []).append(yy_source)
                    # Lưu metadata mới
                    meta_path = os.path.join(story_folder, "metadata.json")
                    with open(meta_path, "w", encoding="utf-8") as f:
                        json.dump(metadata, f, ensure_ascii=False, indent=4)
                    logger.info(f"[SYNC-YY] Tự động thêm nguồn TruyenYY: {yy_url}")
                else:
                    logger.info(f"[SYNC-YY] Không tìm thấy truyện trên TruyenYY: {yy_url}")
                    return
    try:
        yy_adapter = get_adapter("truyenyy")
        chapters_yy = await yy_adapter.get_chapter_list(yy_source["url"], metadata["title"], "truyenyy")
        for idx, ch in enumerate(chapters_yy):
            real_num = extract_real_chapter_number(ch.get('title', '')) or (idx + 1)
            fname = get_chapter_filename(ch.get("title", ""), real_num)
            fpath = os.path.join(story_folder, fname)
            yy_content = await yy_adapter.get_chapter_content(ch["url"], ch["title"], "truyenyy")
            if not yy_content or not yy_content.strip():
                continue
            # Nếu chưa có file, tạo luôn từ yy
            if not os.path.exists(fpath):
                with open(fpath, "w", encoding="utf-8") as f:
                    f.write(yy_content)
                logger.info(f"[SYNC-YY] Tạo mới chương {fname} từ truyenyy")
            else:
                # Nếu file đã tồn tại, check nội dung. Nếu khác, thì overwrite từ yy
                with open(fpath, "r", encoding="utf-8") as f:
                    old_content = f.read()
                if yy_content.strip() != old_content.strip():
                    with open(fpath, "w", encoding="utf-8") as f:
                        f.write(yy_content)
                    logger.info(f"[SYNC-YY] Overwrite lại chương {fname} từ truyenyy")
    except Exception as e:
        logger.warning(f"[SYNC-YY] Lỗi khi đồng bộ chương với truyenyy: {e}")
