import json
import os
from adapters.factory import get_adapter
from utils.logger import logger
from utils.chapter_utils import extract_real_chapter_number, get_chapter_filename
from utils.chapter_utils import slugify_title

def find_all_old_files_by_real_num(story_folder, real_num):
    # Trả về tất cả file có số chương đúng
    matched = []
    for fname in os.listdir(story_folder):
        if fname.startswith(f"{real_num:04d}_") or fname.startswith(f"{real_num:04d}."):
            matched.append(os.path.join(story_folder, fname))
    return matched

async def sync_chapter_with_yy_first_batch(story_folder, metadata):
    yy_source = None
    for src in metadata.get("sources", []):
        if isinstance(src, dict) and (src.get("site_key") == "truyenyy" or src.get("site") == "truyenyy"):
            yy_source = src
            break

    # Nếu chưa có, tự động sinh url YY và check tồn tại
    if not yy_source:
        # Sinh slug
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
        chapters_yy = await yy_adapter.get_chapter_list(
          story_url=yy_source["url"],
          story_title=metadata["title"],
          site_key="truyenyy",
          total_chapters=metadata.get("total_chapters_on_site")
        )
        for idx, ch in enumerate(chapters_yy):
            real_num = extract_real_chapter_number(ch.get('title', '')) or (idx + 1)
            fname = get_chapter_filename(ch.get("title", ""), real_num)
            fpath = os.path.join(story_folder, fname)
            yy_content = await yy_adapter.get_chapter_content(ch["url"], ch["title"], "truyenyy")
            if not yy_content or not yy_content.strip(): #type: ignore
                continue

            # Xóa hết mọi file cũ cùng số chương (ngoại trừ file chuẩn hóa sắp ghi)
            old_files = find_all_old_files_by_real_num(story_folder, real_num)
            for old_f in old_files:
                if os.path.abspath(old_f) != os.path.abspath(fpath):
                    os.remove(old_f)
                    logger.info(f"[SYNC-YY] Đã xóa file trùng số chương: {os.path.basename(old_f)}")

            # Ghi đè file chuẩn hóa (dù có hay không)
            with open(fpath, "w", encoding="utf-8") as f:
                f.write(yy_content)#type: ignore
            logger.info(f"[SYNC-YY] Overwrite/tao moi chương {fname} từ truyenyy")
    except Exception as e:
        logger.warning(f"[SYNC-YY] Lỗi khi đồng bộ chương với truyenyy: {e}")