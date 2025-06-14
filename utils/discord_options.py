import discord
from discord.ext import commands
import asyncio
import os
import json
from adapters.factory import get_adapter
from main import run_single_site, run_single_story
from utils.chapter_utils import slugify_title
from utils.notifier import send_discord_notify
from config.config import BASE_URLS, DISCORD_BOT_TOKEN, DISCORD_USER_ID, DISCORD_SERVER_ID
from workers.clean_garbage import main as clean_garbage_main, clean_error_jsons
from workers.crawler_single_missing_chapter import crawl_single_story_worker
from workers.retry_failed_chapters import retry_queue
from workers.crawler_missing_chapter import (
    check_and_crawl_missing_all_stories,
    loop_once_multi_sites,
)

TOKEN = DISCORD_BOT_TOKEN
GUILD_ID = DISCORD_SERVER_ID
ALLOWED_USER_IDS = [DISCORD_USER_ID]

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='@', intents=intents)
user_state = {}
recrawl_pending = {}

def is_allowed_user(user):
    return True

MAIN_MENU = (
    "**Vui lòng chọn chức năng (gõ số hoặc tên lệnh):**\n"
    "1. Crawl missing ALL sites\n"
    "2. Crawl một site cụ thể\n"
    "3. Chạy các tác vụ worker khác\n"
    "4. Menu phụ\n"
    "5. Hủy\n"
    "6. Crawl single truyện (tên hoặc URL)"
)

EXTRA_MENU = (
    "**Menu phụ:**\n"
    "1. Thống kê completed theo thể loại\n"
    "2. Tìm truyện theo tên\n"
    "3. Recrawl truyện\n"
    "4. Xem meta truyện\n"
    "5. Xem tiến độ crawl truyện\n"
    "6. Hủy\n"
    "7. Đếm số truyện đã crawl trong truyen_data"
)

WORKER_MENU = (
    "**Chọn tác vụ worker:**\n"
    "1. Dọn rác hệ thống\n"
    "2. Retry chương lỗi (chapter_retry_queue)\n"
    "3. Dọn error jsons/missing\n"
    "4. Hủy"
)

@bot.event
async def on_ready():
    print(f'Bot logged in as {bot.user}')

@bot.command(name='start', aliases=['@start'])
async def start(ctx):
    if not is_allowed_user(ctx.author):
        await ctx.send("Không có quyền chạy crawler.")
        return
    user_state[ctx.author.id] = "main_menu"
    await ctx.send(MAIN_MENU)

@bot.event
async def on_message(message):
    if message.author == bot.user:
        return
    uid = message.author.id
    text = message.content.strip()
    text_lower = text.lower().strip()

    # -- Thoát toàn bộ stateful menu
    if text_lower in ["hủy", "thoát", "cancel", "quay lại", "exit"]:
        user_state.pop(uid, None)
        recrawl_pending.pop(uid, None)
        await message.channel.send("Đã hủy thao tác. Nhập @start để quay lại menu.")
        return

    # ==== Crawl single truyện ====
    if user_state.get(uid) == "crawl_single_story":
        query = text
        await message.channel.send(f"Đang crawl truyện: {query}")

        try:
            is_url = query.startswith("http")
            await crawl_single_story_worker(
                story_url=query if is_url else None,
                title=None if is_url else query
            )
            await message.channel.send("✅ Đã crawl xong truyện!")
        except Exception as e:
            await message.channel.send(f"❌ Lỗi crawl: {e}")
        await message.channel.send("Nhập tên hoặc url truyện khác để crawl tiếp hoặc nhập 'hủy' để quay lại menu.")
        return


    # === MAIN MENU ===
    if user_state.get(uid) == "main_menu":
        if text_lower in ["1", "crawl missing all sites"]:
            await message.channel.send("Bắt đầu crawl missing tất cả site...")
            await run_crawl_and_notify_user_discord(message)
            user_state.pop(uid, None)
        elif text_lower in ["2", "crawl một site cụ thể"]:
            user_state[uid] = "choose_site"
            site_list = "\n".join(f"- {k}" for k in BASE_URLS.keys())
            await message.channel.send(f"Chọn site muốn thao tác:\n{site_list}\nNhập đúng tên site.")
        elif text_lower in ["3", "chạy các tác vụ worker khác"]:
            user_state[uid] = "worker_menu"
            await message.channel.send(WORKER_MENU)
        elif text_lower in ["4", "menu phụ"]:
            user_state[uid] = "extra_menu"
            await message.channel.send(EXTRA_MENU)
        elif text_lower in ["5", "hủy", "cancel"]:
            await message.channel.send("Đã hủy thao tác.")
            user_state.pop(uid, None)
        elif text_lower in ["6", "crawl single truyện", "crawl single"]:
            user_state[uid] = "crawl_single_story"
            await message.channel.send("Nhập **tên truyện** hoặc **URL truyện** cần crawl (nhập 'hủy' để quay lại menu):")
            return
        else:
            await message.channel.send("Không hiểu lựa chọn, nhập lại @start")
        return

    # === CHỌN SITE ===
    if user_state.get(uid) == "choose_site":
        site = text
        if site in BASE_URLS:
            user_state[uid] = ("choose_crawl_type", site)
            await message.channel.send(f"Bạn muốn:\n1. Missing chapter - {site}\n2. Full crawl - {site}\n3. Quay lại chọn site\nNhập số hoặc copy dòng.")
        else:
            await message.channel.send("Tên site không hợp lệ. Thử lại!")
        return

    # === CHỌN LOẠI CRAWL ===
    if isinstance(user_state.get(uid), tuple) and user_state[uid][0] == "choose_crawl_type":
        site = user_state[uid][1]
        if text_lower.startswith("1") or text_lower.startswith("missing chapter"):
            adapter = get_adapter(site)
            await message.channel.send(f"Bắt đầu crawl missing chapter cho site: {site}")
            await check_and_crawl_missing_all_stories(adapter, BASE_URLS[site], site)
            user_state.pop(uid, None)
        elif text_lower.startswith("2") or text_lower.startswith("full crawl"):
            await message.channel.send(f"Bắt đầu crawl FULL cho site: {site}")
            try:
                from utils.state_utils import merge_all_missing_workers_to_main
                merge_all_missing_workers_to_main(site)
                await run_single_site(site, crawl_mode="genres_only")
                await send_discord_notify(f"✅ Crawl full site {site} hoàn thành!")
            except Exception as e:
                await send_discord_notify(f"❌ Crawl full site {site} lỗi: {e}")
            user_state.pop(uid, None)
        elif text_lower.startswith("3") or text_lower.startswith("quay lại"):
            user_state[uid] = "choose_site"
            site_list = "\n".join(f"- {k}" for k in BASE_URLS.keys())
            await message.channel.send(f"Chọn site muốn thao tác:\n{site_list}")
        else:
            await message.channel.send("Không hiểu lựa chọn. Hãy nhập lại.")
        return

    # === WORKER MENU ===
    if user_state.get(uid) == "worker_menu":
        if text_lower.startswith("1") or "dọn rác" in text_lower:
            await message.channel.send("Bắt đầu dọn rác hệ thống, vui lòng đợi...")
            await asyncio.to_thread(clean_garbage_main)
            await message.channel.send("✅ Đã dọn rác xong!")
        elif text_lower.startswith("2") or "retry" in text_lower:
            await message.channel.send("Bắt đầu retry chương lỗi...")
            await retry_queue(interval=2)
            await message.channel.send("✅ Đã retry chương lỗi xong!")
        elif text_lower.startswith("3") or "error json" in text_lower:
            await message.channel.send("Bắt đầu dọn error/missing...")
            await asyncio.to_thread(clean_error_jsons)
            await message.channel.send("✅ Đã dọn error json/missing!")
        elif text_lower.startswith("4") or text_lower.startswith("hủy"):
            await message.channel.send("Đã hủy thao tác.")
        else:
            await message.channel.send("Không hiểu lựa chọn. Hãy nhập lại.")
        user_state.pop(uid, None)
        return

    # === EXTRA MENU ===
    if user_state.get(uid) == "extra_menu":
        if text_lower.startswith("1") or "thống kê" in text_lower:
            await count_completed_by_genre_discord(message)
        elif text_lower.startswith("2") or "tìm truyện" in text_lower:
            user_state[uid] = "search_story"
            await message.channel.send("Nhập tên truyện muốn tìm (chỉ tên, nhập 'hủy' để quay lại menu):")
            return
        elif text_lower.startswith("3") or "recrawl" in text_lower:
            user_state[uid] = "recrawl_story"
            await message.channel.send("Nhập tên truyện muốn recrawl (chỉ tên, nhập 'hủy' để quay lại menu):")
            return
        elif text_lower.startswith("4") or "meta" in text_lower:
            user_state[uid] = "meta_story"
            await message.channel.send("Nhập tên truyện muốn xem metadata (chỉ tên, nhập 'hủy' để quay lại menu):")
            return
        elif text_lower.startswith("5") or "tiến độ" in text_lower:
            user_state[uid] = "progress_story"
            await message.channel.send("Nhập tên truyện muốn kiểm tra tiến độ (chỉ tên, nhập 'hủy' để quay lại menu):")
            return
        elif text_lower.startswith("6") or text_lower.startswith("hủy"):
            await message.channel.send("Đã hủy thao tác.")
        elif text_lower.startswith("7") or "đếm số" in text_lower:
            await count_stories_in_truyen_data(message)
        else:
            await message.channel.send("Không hiểu lựa chọn. Hãy nhập lại.")
        user_state.pop(uid, None)
        return

    # === SEARCH STORY - STATEFUL ===
    if user_state.get(uid) == "search_story":
        name = text
        slug = slugify_title(name)
        found_paths = []
        truyen_data_folder = os.path.join("truyen_data", slug)
        if os.path.exists(os.path.join(truyen_data_folder, "metadata.json")):
            found_paths.append(truyen_data_folder)
        if os.path.exists("completed_stories"):
            for genre in os.listdir("completed_stories"):
                genre_folder = os.path.join("completed_stories", genre)
                candidate = os.path.join(genre_folder, slug)
                if os.path.exists(os.path.join(candidate, "metadata.json")):
                    found_paths.append(candidate)
        if not found_paths:
            await message.channel.send("❌ Không tìm thấy truyện!")
        else:
            reply = "Tìm thấy truyện ở các folder:\n" + "\n".join(found_paths)
            await message.channel.send(reply)
        await message.channel.send("Nhập tên truyện khác để kiểm tra tiếp hoặc nhập 'hủy' để quay lại menu.")
        return

    # === RECRAWL STORY - STATEFUL ===
    if user_state.get(uid) == "recrawl_story":
        title = text
        await message.channel.send(f"Đang recrawl truyện: {title}")
        try:
            await run_single_story(title)
            await message.channel.send("✅ Đã recrawl xong!")
        except Exception as e:
            await message.channel.send(f"❌ Recrawl lỗi: {e}")
        await message.channel.send("Nhập tên truyện khác để recrawl tiếp hoặc nhập 'hủy' để quay lại menu.")
        return

    # === META STORY - STATEFUL ===
    if user_state.get(uid) == "meta_story":
        name = text
        slug = slugify_title(name)
        found = False
        meta_paths = [os.path.join("truyen_data", slug, "metadata.json")]
        if os.path.exists("completed_stories"):
            for genre in os.listdir("completed_stories"):
                genre_folder = os.path.join("completed_stories", genre)
                meta_paths.append(os.path.join(genre_folder, slug, "metadata.json"))
        for meta_path in meta_paths:
            if os.path.exists(meta_path):
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                reply = f"**Metadata truyện:**\n```json\n{json.dumps(meta, ensure_ascii=False, indent=2)}\n```"
                await message.channel.send(reply)
                found = True
                break
        if not found:
            await message.channel.send("❌ Không tìm thấy metadata!")
        await message.channel.send("Nhập tên truyện khác để kiểm tra tiếp hoặc nhập 'hủy' để quay lại menu.")
        return

    # === PROGRESS STORY - STATEFUL ===
    if user_state.get(uid) == "progress_story":
        name = text
        slug = slugify_title(name)
        found = False
        truyen_data_folder = os.path.join("truyen_data", slug)
        meta_paths = [os.path.join(truyen_data_folder, "metadata.json")]
        if os.path.exists("completed_stories"):
            for genre in os.listdir("completed_stories"):
                genre_folder = os.path.join("completed_stories", genre)
                meta_paths.append(os.path.join(genre_folder, slug, "metadata.json"))
        for meta_path in meta_paths:
            if os.path.exists(meta_path):
                story_folder = os.path.dirname(meta_path)
                num_txt = len([f for f in os.listdir(story_folder) if f.endswith('.txt')])
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                total_chapters = meta.get("total_chapters_on_site", "?")
                reply = (
                    f"Tiến độ truyện: **{meta.get('title')}**\n"
                    f"Đã crawl: **{num_txt}** chương / Tổng số chương trên site: **{total_chapters}**"
                )
                await message.channel.send(reply)
                found = True
                break
        if not found:
            await message.channel.send("❌ Không tìm thấy truyện hoặc chưa có metadata!")
        await message.channel.send("Nhập tên truyện khác để kiểm tra tiếp hoặc nhập 'hủy' để quay lại menu.")
        return

    await bot.process_commands(message)

# ---- Worker: Thống kê completed ----
async def count_completed_by_genre_discord(message):
    completed_root = "completed_stories"
    result_lines = []
    if not os.path.exists(completed_root):
        await message.channel.send("❌ Chưa có thư mục completed_stories!")
        return
    genres = [g for g in os.listdir(completed_root) if os.path.isdir(os.path.join(completed_root, g))]
    if not genres:
        await message.channel.send("❌ Chưa có thể loại nào trong completed_stories!")
        return
    for genre in genres:
        genre_path = os.path.join(completed_root, genre)
        count = 0
        for name in os.listdir(genre_path):
            story_folder = os.path.join(genre_path, name)
            if os.path.isdir(story_folder) and os.path.exists(os.path.join(story_folder, "metadata.json")):
                count += 1
        result_lines.append(f"{genre}: {count}")
    reply = "\n".join(result_lines) if result_lines else "Chưa có truyện nào completed."
    await message.channel.send(reply)

# ---- Crawl all sites cho Discord ----
async def run_crawl_and_notify_user_discord(message):
    try:
        await loop_once_multi_sites(force_unskip=False)
        await message.channel.send("✅ Đã crawl/check missing xong toàn bộ!")
    except Exception as ex:
        await message.channel.send(f"❌ Crawl gặp lỗi: {ex}")

async def count_stories_in_truyen_data(message):
    truyen_data_root = "truyen_data"
    if not os.path.exists(truyen_data_root):
        await message.channel.send("❌ Thư mục truyen_data không tồn tại!")
        return
    count = 0
    for name in os.listdir(truyen_data_root):
        story_folder = os.path.join(truyen_data_root, name)
        if os.path.isdir(story_folder) and os.path.exists(os.path.join(story_folder, "metadata.json")):
            count += 1
    await message.channel.send(f"Hiện tại đã crawl được **{count} truyện** trong truyen_data.")

bot.run(TOKEN)
