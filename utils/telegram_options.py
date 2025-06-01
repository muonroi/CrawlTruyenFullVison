

import asyncio
import json
from mailbox import Message
import os
from timeit import main
from adapters.factory import get_adapter
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram import  types, F, Router
from clean_garbage import clean_error_jsons
from config.config import BASE_URLS, TELEGRAM_CHAT_ID
from workers.crawler_single_missing_chapter import check_and_crawl_missing_all_stories, loop_once_multi_sites
from main import retry_failed_genres, run_crawler
from retry_failed_chapters import retry_queue
from utils.logger import logger
from utils.chapter_utils import slugify_title
from utils.notifier import send_telegram_notify

telegram_router = Router()
is_crawling = False
user_state = {}
active_crawl_tasks = {}

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
            [KeyboardButton(text="Xem tiến độ crawl truyện")], 
            [KeyboardButton(text="Hủy")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await message.reply("Chọn chức năng:", reply_markup=kb)


@telegram_router.message(F.text == "Xem tiến độ crawl truyện")
async def ask_progress_story(message: types.Message):
    user_state[message.from_user.id] = "progress_story"  # Đặt state riêng #type:ignore
    await message.reply("Nhập tên truyện muốn kiểm tra tiến độ (chỉ tên):", reply_markup=ReplyKeyboardRemove())



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
    uid = message.from_user.id  # type: ignore
    if uid not in user_state:
        return

    name = message.text.strip() # type: ignore
    slug = slugify_title(name)

    state = user_state[uid]

    if state == "search_story":
        # --- LOGIC TÌM TRUYỆN THEO TÊN ---
        found_paths = []
        # Tìm trong truyen_data
        truyen_data_folder = os.path.join("truyen_data", slug)
        if os.path.exists(os.path.join(truyen_data_folder, "metadata.json")):
            found_paths.append(truyen_data_folder)
        # Tìm trong completed_stories
        if os.path.exists("completed_stories"):
            for genre in os.listdir("completed_stories"):
                genre_folder = os.path.join("completed_stories", genre)
                candidate = os.path.join(genre_folder, slug)
                if os.path.exists(os.path.join(candidate, "metadata.json")):
                    found_paths.append(candidate)
        if not found_paths:
            await message.reply("❌ Không tìm thấy truyện!", reply_markup=ReplyKeyboardRemove())
        else:
            reply = "Tìm thấy truyện ở các folder:\n" + "\n".join(found_paths)
            await message.reply(reply, reply_markup=ReplyKeyboardRemove())
        user_state.pop(uid, None)
        await handle_start_crawl(message)
        return

    if state == "meta_story":
        # --- LOGIC XEM METADATA TRUYỆN ---
        found = False
        # Tìm trong truyen_data
        meta_paths = [os.path.join("truyen_data", slug, "metadata.json")]
        # Tìm trong completed_stories
        if os.path.exists("completed_stories"):
            for genre in os.listdir("completed_stories"):
                genre_folder = os.path.join("completed_stories", genre)
                meta_paths.append(os.path.join(genre_folder, slug, "metadata.json"))
        for meta_path in meta_paths:
            if os.path.exists(meta_path):
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                reply = f"**Metadata truyện:**\n```\n{json.dumps(meta, ensure_ascii=False, indent=2)}\n```"
                await message.reply(reply, reply_markup=ReplyKeyboardRemove())
                found = True
                break
        if not found:
            await message.reply("❌ Không tìm thấy metadata!", reply_markup=ReplyKeyboardRemove())
        user_state.pop(uid, None)
        await handle_start_crawl(message)
        return
    if state == "progress_story":
        found = False
        truyen_data_folder = os.path.join("truyen_data", slug)
        meta_paths = [os.path.join(truyen_data_folder, "metadata.json")]

        # Tìm trong completed_stories
        if os.path.exists("completed_stories"):
            for genre in os.listdir("completed_stories"):
                genre_folder = os.path.join("completed_stories", genre)
                meta_paths.append(os.path.join(genre_folder, slug, "metadata.json"))

        for meta_path in meta_paths:
            if os.path.exists(meta_path):
                # Đếm số file .txt trong folder này
                story_folder = os.path.dirname(meta_path)
                num_txt = len([f for f in os.listdir(story_folder) if f.endswith('.txt')])
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                total_chapters = meta.get("total_chapters_on_site", "?")
                reply = (
                    f"Tiến độ truyện: <b>{meta.get('title')}</b>\n"
                    f"Đã crawl: <b>{num_txt}</b> chương / Tổng số chương trên site: <b>{total_chapters}</b>"
                )
                await message.reply(reply, parse_mode="HTML", reply_markup=ReplyKeyboardRemove())
                found = True
                break

        if not found:
            await message.reply("❌ Không tìm thấy truyện hoặc chưa có metadata!", reply_markup=ReplyKeyboardRemove())

        user_state.pop(uid, None)
        await handle_start_crawl(message)
        return

def normalize_chapter_item(ch):
    """Đảm bảo item chương là dict, luôn có title và url."""
    if isinstance(ch, dict):
        title = ch.get('title', 'untitled')
        url = ch.get('url', None)
        ch['title'] = title
        ch['url'] = url
        return ch
    elif isinstance(ch, str):
        logger.warning(f"[WARNING] Chương nhận về là str, không phải dict! Dữ liệu: {ch[:100]}")
        return {'title': 'untitled', 'url': ch}
    else:
        logger.warning(f"[WARNING] Chương nhận về kiểu {type(ch)}, bỏ qua: {ch}")
        return {'title': 'untitled', 'url': None}


@telegram_router.message(F.text.regexp(r"^Recrawl nguồn "))
async def recrawl_story_choose_site(message: types.Message):
    uid = message.from_user.id # type: ignore
    if uid not in user_state:
        return
    state = user_state[uid]
    if not (isinstance(state, tuple) and state[0] == "recrawl_choose_site"):
        return
    site_key = message.text.replace("Recrawl nguồn ", "").strip() # type: ignore
    slug, folder, meta = state[1], state[2], state[3]
    try:
        adapter = get_adapter(site_key)
        chapters = await adapter.get_chapter_list(meta["url"], meta["title"], site_key,total_chapters=meta.get("total_chapters_on_site", 0))
        from workers.crawler_single_missing_chapter import crawl_missing_chapters_for_story
        await crawl_missing_chapters_for_story(site_key, None, chapters, meta, {}, folder, {}, 10)
        await message.reply(f"✅ Đã recrawl lại truyện '{meta['title']}' ({slug}) theo nguồn {site_key}!")
    except Exception as ex:
        await message.reply(f"❌ Lỗi recrawl: {ex}")
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
async def crawl_all_sites(message: Message):
    uid = message.from_user.id# type: ignore
    if uid in active_crawl_tasks and not active_crawl_tasks[uid].done():
        await message.reply("Đang có 1 tác vụ crawl running. Vui lòng đợi.")# type: ignore
        return
    await message.reply("Bắt đầu crawl missing tất cả site...", reply_markup=ReplyKeyboardRemove())# type: ignore
    task = asyncio.create_task(run_crawl_and_notify_user(message))
    active_crawl_tasks[uid] = task

async def run_crawl_and_notify_user(message: Message):
    try:
        await loop_once_multi_sites(force_unskip=False)
        await message.reply("✅ Đã crawl/check missing xong toàn bộ!", reply_markup=ReplyKeyboardRemove()) # type: ignore
    except Exception as ex:
        await message.reply(f"❌ Crawl gặp lỗi: {ex}", reply_markup=ReplyKeyboardRemove())# type: ignore

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
    completed_root = "completed_stories"
    result_lines = []

    if not os.path.exists(completed_root):
        await message.reply("❌ Chưa có thư mục completed_stories!", reply_markup=ReplyKeyboardRemove())
        return

    genres = [g for g in os.listdir(completed_root) if os.path.isdir(os.path.join(completed_root, g))]
    if not genres:
        await message.reply("❌ Chưa có thể loại nào trong completed_stories!", reply_markup=ReplyKeyboardRemove())
        return

    for genre in genres:
        genre_path = os.path.join(completed_root, genre)
        # Đếm số truyện (folder) có file metadata.json bên trong
        count = 0
        for name in os.listdir(genre_path):
            story_folder = os.path.join(genre_path, name)
            if os.path.isdir(story_folder) and os.path.exists(os.path.join(story_folder, "metadata.json")):
                count += 1
        result_lines.append(f"{genre}: {count}")

    reply = "\n".join(result_lines) if result_lines else "Chưa có truyện nào completed."
    await message.reply(reply, reply_markup=ReplyKeyboardRemove())
 

