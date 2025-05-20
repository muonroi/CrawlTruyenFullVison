

import asyncio
import json
import os
from timeit import main
from adapters.factory import get_adapter
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram import  types, F, Router
from clean_garbage import clean_error_jsons
from config.config import BASE_URLS, TELEGRAM_CHAT_ID
from core.crawler_missing_chapter import check_and_crawl_missing_all_stories, loop_once_multi_sites
from main import retry_failed_genres, run_crawler
from retry_failed_chapters import retry_queue
from utils.chapter_utils import slugify_title
from utils.notifier import send_telegram_notify

telegram_router = Router()
is_crawling = False
user_state = {}

@telegram_router.message(F.text == '/start')
async def handle_start_crawl(message: types.Message):
    if str(message.chat.id) != TELEGRAM_CHAT_ID:
        await message.reply("Không có quyền chạy crawler.")
        return

    kb_main = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Crawl missing ALL sites")],
            [KeyboardButton(text="Crawl một site cụ thể")],
            [KeyboardButton(text="Chạy các tác vụ worker khác")],
            [KeyboardButton(text="Menu phụ")],
            [KeyboardButton(text="Hủy")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await message.reply("Vui lòng chọn chức năng:", reply_markup=kb_main)

@telegram_router.message(F.text == "Menu phụ")
async def show_extra_menu(message: types.Message):
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Thống kê completed theo thể loại")],
            [KeyboardButton(text="Tìm truyện theo tên")],
            [KeyboardButton(text="Recrawl truyện")],
            [KeyboardButton(text="Xem meta truyện")],
            [KeyboardButton(text="Hủy")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await message.reply("Chọn chức năng:", reply_markup=kb)
@telegram_router.message(F.text == "Tìm truyện theo tên")
async def ask_search_story(message: types.Message):
    user_state[message.from_user.id] = "search_story"#type:ignore
    await message.reply("Vui lòng nhập tên truyện muốn tìm (chỉ tên, không cần 'Tìm truyện ...'):", reply_markup=ReplyKeyboardRemove())

@telegram_router.message(F.text == "Recrawl truyện")
async def ask_recrawl_story(message: types.Message):
    user_state[message.from_user.id] = "recrawl_story"#type:ignore
    await message.reply("Nhập tên truyện muốn recrawl (chỉ tên):", reply_markup=ReplyKeyboardRemove())

@telegram_router.message(F.text == "Xem meta truyện")
async def ask_meta_story(message: types.Message):
    user_state[message.from_user.id] = "meta_story"#type:ignore
    await message.reply("Nhập tên truyện muốn xem metadata (chỉ tên):", reply_markup=ReplyKeyboardRemove())

@telegram_router.message()
async def handle_text_input(message: types.Message):
    uid = message.from_user.id#type:ignore
    if uid not in user_state:
        return  # Không có state gì, có thể trả về menu hoặc bỏ qua

    name = message.text.strip() #type:ignore
    slug = slugify_title(name)

    if uid in user_state and user_state[uid] == "recrawl_story":
        from core.crawler_missing_chapter import crawl_missing_chapters_for_story
        name = message.text.strip() #type:ignore
        slug = slugify_title(name)
        for folder in [os.path.join("truyen_data", slug)] + [
            os.path.join("completed_stories", genre, slug) for genre in os.listdir("completed_stories")
        ]:
            meta_path = os.path.join(folder, "metadata.json")
            if os.path.exists(meta_path):
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                # --- Lấy site_key ---
                site_key = meta.get("site_key")
                sources = meta.get("sources", [])
                if not site_key:
                    if isinstance(sources, list) and len(sources) == 1:
                        site_key = sources[0].get("site")
                    elif isinstance(sources, list) and len(sources) > 1:
                        # Yêu cầu người dùng chọn site
                        user_state[uid] = ("recrawl_choose_site", slug, folder, meta)
                        # Tạo menu chọn site từ sources
                        buttons = [
                            [KeyboardButton(text=f"Recrawl nguồn {s.get('site', 'unknown')}")] for s in sources
                        ]
                        kb = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, one_time_keyboard=True)
                        await message.reply(
                            "Truyện này có nhiều nguồn. Chọn nguồn muốn recrawl:",
                            reply_markup=kb
                        )
                        return
                if not site_key:
                    await message.reply("Không xác định được site_key trong metadata.\nNội dung meta:\n" + json.dumps(meta, ensure_ascii=False, indent=2))
                    user_state.pop(uid, None)
                    return
                adapter = get_adapter(site_key)
                chapters = await adapter.get_chapter_list(meta["url"], meta["title"])
                await crawl_missing_chapters_for_story(site_key, None, chapters, meta, {}, folder, {}, 10)
                await message.reply(f"Đã recrawl lại truyện '{name}' ({slug})!")
                user_state.pop(uid, None)
                return
        await message.reply("Không tìm thấy truyện hoặc metadata!", reply_markup=ReplyKeyboardRemove())
        user_state.pop(uid, None)
        return

@telegram_router.message(F.text.regexp(r"^Recrawl nguồn "))
async def recrawl_story_choose_site(message: types.Message):
    uid = message.from_user.id#type:ignore
    if uid not in user_state:
        return
    state = user_state[uid]
    if not (isinstance(state, tuple) and state[0] == "recrawl_choose_site"):
        return
    site_key = message.text.replace("Recrawl nguồn ", "").strip() #type:ignore
    slug, folder, meta = state[1], state[2], state[3]
    adapter = get_adapter(site_key)
    chapters = await adapter.get_chapter_list(meta["url"], meta["title"])
    from core.crawler_missing_chapter import crawl_missing_chapters_for_story
    await crawl_missing_chapters_for_story(site_key, None, chapters, meta, {}, folder, {}, 10)
    await message.reply(f"Đã recrawl lại truyện '{meta['title']}' ({slug}) theo nguồn {site_key}!")
    user_state.pop(uid, None)

@telegram_router.message(F.text == "Chạy các tác vụ worker khác")
async def show_worker_menu(message: types.Message):
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Dọn rác hệ thống")],
            [KeyboardButton(text="Retry chương lỗi (chapter_retry_queue)")],
            [KeyboardButton(text="Dọn error jsons/missing")],
            [KeyboardButton(text="Hủy")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await message.reply("Chọn tác vụ worker muốn chạy:", reply_markup=kb)

async def async_clean_worker():
    # Bọc hàm dọn rác gốc vào to_thread (nếu là sync code)
    await asyncio.to_thread(main)  # main là hàm dọn rác sync đã có sẵn

@telegram_router.message(F.text == "Retry chương lỗi (chapter_retry_queue)")
async def run_retry_queue(message: types.Message):
    await message.reply("Bắt đầu retry chương lỗi...", reply_markup=ReplyKeyboardRemove())
    # Chạy 1 lần rồi stop (nếu muốn chạy mãi thì nên chạy song song dưới dạng task)
    await retry_queue(interval=2)  # có thể chỉnh interval cho phù hợp, vd chạy thử 1 lần thôi
    await message.reply("✅ Đã retry chương lỗi xong!")

async def async_clean_error_jsons():
    await asyncio.to_thread(clean_error_jsons)  # Hàm này là sync, nên dùng to_thread

@telegram_router.message(F.text == "Dọn error jsons/missing")
async def run_clean_error_jsons(message: types.Message):
    await message.reply("Bắt đầu dọn error/missing...", reply_markup=ReplyKeyboardRemove())
    await async_clean_error_jsons()
    await message.reply("✅ Đã dọn error json/missing!")

@telegram_router.message(F.text == "Dọn rác hệ thống")
async def run_clean_worker(message: types.Message):
    await message.reply("Bắt đầu dọn rác hệ thống, vui lòng đợi...", reply_markup=ReplyKeyboardRemove())
    await async_clean_worker()
    await message.reply("✅ Đã dọn rác xong!")

# ---- Crawl tất cả site (Missing) ----
@telegram_router.message(F.text == "Crawl missing ALL sites")
async def crawl_all_sites(message: types.Message):
    await message.reply("Bắt đầu crawl missing tất cả site...", reply_markup=ReplyKeyboardRemove())
    await loop_once_multi_sites(force_unskip=False)

# ---- Bắt đầu chọn 1 site cụ thể ----
@telegram_router.message(F.text == "Crawl một site cụ thể")
async def choose_site(message: types.Message):
    site_buttons = [
        [KeyboardButton(text=site)] for site in BASE_URLS.keys()
    ]
    kb = ReplyKeyboardMarkup(keyboard=site_buttons, resize_keyboard=True, one_time_keyboard=True)
    await message.reply("Chọn site muốn thao tác:", reply_markup=kb)

# ---- Sau khi chọn site, hỏi muốn crawl kiểu gì ----
@telegram_router.message(lambda message: message.text in BASE_URLS.keys())
async def choose_crawl_type(message: types.Message):
    site_key = message.text
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=f"Missing chapter - {site_key}")],
            [KeyboardButton(text=f"Full crawl - {site_key}")],
            [KeyboardButton(text="Quay lại chọn site")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await message.reply(
        f"Chọn chế độ crawl cho {site_key}:",
        reply_markup=kb
    )

# ---- Crawl missing cho 1 site ----
@telegram_router.message(lambda message: message.text.startswith("Missing chapter - "))
async def crawl_missing_for_site(message: types.Message):
    site_key = message.text.replace("Missing chapter - ", "") #type:ignore
    adapter = get_adapter(site_key)
    await message.reply(f"Bắt đầu crawl missing chapter cho site: {site_key}", reply_markup=ReplyKeyboardRemove())
    await check_and_crawl_missing_all_stories(adapter, BASE_URLS[site_key], site_key)

# ---- Crawl FULL cho 1 site (chạy như main.py CLI) ----
@telegram_router.message(lambda message: message.text.startswith("Full crawl - "))
async def crawl_full_for_site(message: types.Message):
    global is_crawling
    if is_crawling:
        await message.reply("Crawler đang chạy, vui lòng đợi hoàn thành.", reply_markup=ReplyKeyboardRemove())
        return
    is_crawling = True
    site_key = message.text.replace("Full crawl - ", "") #type:ignore
    adapter = get_adapter(site_key)
    await message.reply(f"Bắt đầu crawl FULL cho site: {site_key}", reply_markup=ReplyKeyboardRemove())
    try:
        from utils.state_utils import merge_all_missing_workers_to_main
        merge_all_missing_workers_to_main(site_key)
        await run_crawler(adapter, site_key)
        await retry_failed_genres(adapter, site_key)
        await send_telegram_notify(f"✅ Crawl full site {site_key} hoàn thành!")
    except Exception as e:
        await send_telegram_notify(f"❌ Crawl full site {site_key} lỗi: {e}")
    finally:
        is_crawling = False

# ---- Quay lại chọn site ----
@telegram_router.message(F.text == "Quay lại chọn site")
async def back_choose_site(message: types.Message):
    site_buttons = [
        [KeyboardButton(text=site)] for site in BASE_URLS.keys()
    ]
    kb = ReplyKeyboardMarkup(keyboard=site_buttons, resize_keyboard=True, one_time_keyboard=True)
    await message.reply("Chọn lại site muốn thao tác:", reply_markup=kb)

@telegram_router.message(F.text == "Hủy")
async def cancel_command(message: types.Message):
    await message.reply("Đã hủy thao tác.", reply_markup=ReplyKeyboardRemove())


@telegram_router.message(F.text == "Thống kê completed theo thể loại")
async def count_completed_by_genre(message: types.Message):
    result_lines = []
    for genre in os.listdir("completed_stories"):
        genre_path = os.path.join("completed_stories", genre)
        if os.path.isdir(genre_path):
            count = sum(os.path.isdir(os.path.join(genre_path, name)) for name in os.listdir(genre_path))
            result_lines.append(f"{genre}: {count}")
    reply = "\n".join(result_lines) if result_lines else "Chưa có truyện nào completed."
    await message.reply(reply, reply_markup=ReplyKeyboardRemove())

