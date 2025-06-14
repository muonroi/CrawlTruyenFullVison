import discord
from discord.ext import commands
import asyncio
import os
import json
from adapters.factory import get_adapter
from main import retry_failed_genres, run_crawler
from utils.chapter_utils import slugify_title
from utils.notifier import send_discord_notify
from config.config import BASE_URLS, DISCORD_BOT_TOKEN

TOKEN = DISCORD_BOT_TOKEN
GUILD_ID = 123456789012345678  # Thay bằng ID server
ALLOWED_USER_IDS = [123456789012345678]  # List user discord id cho phép dùng bot (nếu muốn bảo mật)

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='/', intents=intents)

user_state = {}

def is_allowed_user(user):
    # Nếu muốn kiểm tra quyền (tùy chọn)
    return user.id in ALLOWED_USER_IDS or len(ALLOWED_USER_IDS) == 0

# ====== MENU GỬI TEXT ======
MAIN_MENU = (
    "**Vui lòng chọn chức năng (gõ số hoặc tên lệnh):**\n"
    "1. Crawl missing ALL sites\n"
    "2. Crawl một site cụ thể\n"
    "3. Chạy các tác vụ worker khác\n"
    "4. Menu phụ\n"
    "5. Hủy"
)

EXTRA_MENU = (
    "**Menu phụ:**\n"
    "1. Thống kê completed theo thể loại\n"
    "2. Tìm truyện theo tên\n"
    "3. Recrawl truyện\n"
    "4. Xem meta truyện\n"
    "5. Xem tiến độ crawl truyện\n"
    "6. Hủy"
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

@bot.command(name='start')
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

    # ==== STATE MAIN MENU ====
    if user_state.get(uid) == "main_menu":
        text = message.content.lower().strip()
        if text in ["1", "crawl missing all sites"]:
            await message.channel.send("Bắt đầu crawl missing tất cả site...")
            await run_crawl_and_notify_user_discord(message)
            user_state.pop(uid, None)
            return
        elif text in ["2", "crawl một site cụ thể"]:
            user_state[uid] = "choose_site"
            site_list = "\n".join(f"- {k}" for k in BASE_URLS.keys())
            await message.channel.send(f"Chọn site muốn thao tác:\n{site_list}\nNhập đúng tên site.")
            return
        elif text in ["3", "chạy các tác vụ worker khác"]:
            user_state[uid] = "worker_menu"
            await message.channel.send(WORKER_MENU)
            return
        elif text in ["4", "menu phụ"]:
            user_state[uid] = "extra_menu"
            await message.channel.send(EXTRA_MENU)
            return
        elif text in ["5", "hủy", "cancel"]:
            await message.channel.send("Đã hủy thao tác.")
            user_state.pop(uid, None)
            return
        else:
            await message.channel.send("Không hiểu lựa chọn, nhập lại /start")
            return

    # ==== CHỌN SITE ====
    if user_state.get(uid) == "choose_site":
        site = message.content.strip()
        if site in BASE_URLS:
            user_state[uid] = ("choose_crawl_type", site)
            await message.channel.send(f"Bạn muốn:\n1. Missing chapter - {site}\n2. Full crawl - {site}\n3. Quay lại chọn site\nNhập số hoặc copy dòng.")
        else:
            await message.channel.send("Tên site không hợp lệ. Thử lại!")
        return

    # ==== CHỌN LOẠI CRAWL ====
    if isinstance(user_state.get(uid), tuple) and user_state[uid][0] == "choose_crawl_type":
        site = user_state[uid][1]
        text = message.content.lower().strip()
        if text.startswith("1") or text.startswith("missing chapter"):
            adapter = get_adapter(site)
            await message.channel.send(f"Bắt đầu crawl missing chapter cho site: {site}")
            await check_and_crawl_missing_all_stories(adapter, BASE_URLS[site], site)
            user_state.pop(uid, None)
            return
        elif text.startswith("2") or text.startswith("full crawl"):
            adapter = get_adapter(site)
            await message.channel.send(f"Bắt đầu crawl FULL cho site: {site}")
            try:
                from utils.state_utils import merge_all_missing_workers_to_main
                merge_all_missing_workers_to_main(site)
                await run_crawler(adapter, site)
                await retry_failed_genres(adapter, site)
                await send_discord_notify(f"✅ Crawl full site {site} hoàn thành!")
            except Exception as e:
                await send_discord_notify(f"❌ Crawl full site {site} lỗi: {e}")
            user_state.pop(uid, None)
            return
        elif text.startswith("3") or text.startswith("quay lại"):
            user_state[uid] = "choose_site"
            site_list = "\n".join(f"- {k}" for k in BASE_URLS.keys())
            await message.channel.send(f"Chọn site muốn thao tác:\n{site_list}")
            return
        else:
            await message.channel.send("Không hiểu lựa chọn. Hãy nhập lại.")
        return

    # ==== WORKER MENU ====
    if user_state.get(uid) == "worker_menu":
        text = message.content.lower().strip()
        if text.startswith("1") or "dọn rác" in text:
            await message.channel.send("Bắt đầu dọn rác hệ thống, vui lòng đợi...")
            await asyncio.to_thread(main)
            await message.channel.send("✅ Đã dọn rác xong!")
        elif text.startswith("2") or "retry" in text:
            await message.channel.send("Bắt đầu retry chương lỗi...")
            await retry_queue(interval=2)
            await message.channel.send("✅ Đã retry chương lỗi xong!")
        elif text.startswith("3") or "error json" in text:
            await message.channel.send("Bắt đầu dọn error/missing...")
            await asyncio.to_thread(clean_error_jsons)
            await message.channel.send("✅ Đã dọn error json/missing!")
        elif text.startswith("4") or text.startswith("hủy"):
            await message.channel.send("Đã hủy thao tác.")
        else:
            await message.channel.send("Không hiểu lựa chọn. Hãy nhập lại.")
        user_state.pop(uid, None)
        return

    # ==== EXTRA MENU ====
    if user_state.get(uid) == "extra_menu":
        text = message.content.lower().strip()
        if text.startswith("1") or "thống kê" in text:
            await count_completed_by_genre_discord(message)
        elif text.startswith("2") or "tìm truyện" in text:
            user_state[uid] = "search_story"
            await message.channel.send("Vui lòng nhập tên truyện muốn tìm (chỉ tên):")
            return
        elif text.startswith("3") or "recrawl" in text:
            user_state[uid] = "recrawl_story"
            await message.channel.send("Nhập tên truyện muốn recrawl (chỉ tên):")
            return
        elif text.startswith("4") or "meta" in text:
            user_state[uid] = "meta_story"
            await message.channel.send("Nhập tên truyện muốn xem metadata (chỉ tên):")
            return
        elif text.startswith("5") or "tiến độ" in text:
            user_state[uid] = "progress_story"
            await message.channel.send("Nhập tên truyện muốn kiểm tra tiến độ (chỉ tên):")
            return
        elif text.startswith("6") or text.startswith("hủy"):
            await message.channel.send("Đã hủy thao tác.")
        else:
            await message.channel.send("Không hiểu lựa chọn. Hãy nhập lại.")
        user_state.pop(uid, None)
        return

    # ==== SEARCH STORY ====
    if user_state.get(uid) == "search_story":
        name = message.content.strip()
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
        user_state.pop(uid, None)
        await start(message)
        return

    # ==== META STORY ====
    if user_state.get(uid) == "meta_story":
        name = message.content.strip()
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
        user_state.pop(uid, None)
        await start(message)
        return

    # ==== PROGRESS STORY ====
    if user_state.get(uid) == "progress_story":
        name = message.content.strip()
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
        user_state.pop(uid, None)
        await start(message)
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

# ==== Chạy bot ====
bot.run(TOKEN)
