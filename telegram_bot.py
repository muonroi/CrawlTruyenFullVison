
import asyncio
import os
import glob
import time
import signal
from collections import Counter
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from utils.logger import logger
from utils.kafka_producer import send_kafka_job, stop_kafka_producer
from config.config import TELEGRAM_BOT_TOKEN, LOG_FOLDER, BASE_URLS, DATA_FOLDER, COMPLETED_FOLDER
from utils.story_analyzer import get_all_stories, get_health_stats, get_disk_usage

# --- Helper for sending long messages ---
async def send_in_chunks(update: Update, text: str, max_chars: int = 4000):
    """Sends a long message in chunks to avoid hitting Telegram's limit."""
    message = update.effective_message
    if not message:
        return
    if not text:
        await message.reply_text("Không có dữ liệu để hiển thị.")
        return
    for i in range(0, len(text), max_chars):
        chunk = text[i:i+max_chars]
        await message.reply_html(f"<pre>{chunk}</pre>")


# --- Menu Helpers ---

def build_main_menu() -> InlineKeyboardMarkup:
    """Creates the main inline keyboard menu."""
    keyboard = [
        [InlineKeyboardButton("ℹ️ Trạng thái hệ thống", callback_data="action_status")],
        [InlineKeyboardButton("🚀 Build & Push Image", callback_data="action_build")],
        [InlineKeyboardButton("🕷️ Crawl dữ liệu", callback_data="submenu_crawl")],
        [InlineKeyboardButton("📚 Danh sách truyện", callback_data="submenu_list")],
        [InlineKeyboardButton("📊 Thống kê", callback_data="submenu_stats")],
        [InlineKeyboardButton("🧾 Kiểm tra chương thiếu", callback_data="action_check_missing")],
        [InlineKeyboardButton("🔄 Retry site lỗi", callback_data="input_retry_failed")],
        [InlineKeyboardButton("🪵 Xem logs", callback_data="input_get_logs")],
    ]
    return InlineKeyboardMarkup(keyboard)


async def show_main_menu(update: Update, text: str | None = None, edit: bool = False) -> None:
    """Displays the main action menu either by editing or replying."""
    menu_text = text or "👇 Chọn một chức năng để tiếp tục:"
    markup = build_main_menu()
    callback_query = update.callback_query

    if edit and callback_query:
        try:
            await callback_query.edit_message_text(menu_text, reply_markup=markup)
        except BadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                raise
    else:
        message = update.effective_message
        if message:
            await message.reply_text(menu_text, reply_markup=markup)


def build_crawl_menu() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("🌐 Crawl toàn bộ site", callback_data="action_crawl_all")],
        [InlineKeyboardButton("🧩 Crawl missing only", callback_data="action_crawl_missing")],
        [InlineKeyboardButton("🔗 Crawl truyện theo URL", callback_data="input_crawl_story")],
        [InlineKeyboardButton("🏠 Crawl theo site", callback_data="input_crawl_site")],
        [InlineKeyboardButton("⬅️ Quay lại", callback_data="menu_main")],
    ]
    return InlineKeyboardMarkup(keyboard)


def build_list_menu() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("📦 Tổng quan thư viện", callback_data="action_list_summary")],
        [InlineKeyboardButton("✅ Truyện đã hoàn thành", callback_data="action_list_completed")],
        [InlineKeyboardButton("📚 Danh sách thể loại", callback_data="action_list_genres")],
        [InlineKeyboardButton("⬅️ Quay lại", callback_data="menu_main")],
    ]
    return InlineKeyboardMarkup(keyboard)


def build_stats_menu() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("❤️ Sức khỏe hệ thống", callback_data="action_stats_health")],
        [InlineKeyboardButton("💾 Dung lượng lưu trữ", callback_data="action_stats_disk")],
        [InlineKeyboardButton("🏆 Top thể loại", callback_data="action_stats_top_genres")],
        [InlineKeyboardButton("📖 Truyện dài nhất", callback_data="action_stats_longest")],
        [InlineKeyboardButton("⬅️ Quay lại", callback_data="menu_main")],
    ]
    return InlineKeyboardMarkup(keyboard)


PENDING_ACTION_KEY = "pending_action"


async def prompt_for_input(update: Update, context: ContextTypes.DEFAULT_TYPE, action_key: str, prompt_text: str) -> None:
    """Stores the pending action and prompts the user for additional input."""
    context.user_data[PENDING_ACTION_KEY] = action_key
    message = update.effective_message
    if message:
        await message.reply_text(f"{prompt_text}\nGõ /cancel để hủy thao tác.")
    await show_main_menu(update, edit=True)


async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles button presses from the inline menu."""
    query = update.callback_query
    if not query:
        return

    await query.answer()
    data = query.data

    if data == "submenu_crawl":
        await query.edit_message_text("🕷️ Chọn chế độ crawl:", reply_markup=build_crawl_menu())
        return
    if data == "submenu_list":
        await query.edit_message_text("📚 Chọn loại danh sách cần xem:", reply_markup=build_list_menu())
        return
    if data == "submenu_stats":
        await query.edit_message_text("📊 Chọn loại thống kê:", reply_markup=build_stats_menu())
        return
    if data == "menu_main":
        await show_main_menu(update, edit=True)
        return

    if data == "action_status":
        await status_command(update, context)
        await show_main_menu(update, edit=True)
        return
    if data == "action_build":
        await build_command(update, context)
        await show_main_menu(update, edit=True)
        return
    if data == "action_check_missing":
        await check_missing_command(update, context)
        await show_main_menu(update, edit=True)
        return

    if data == "action_crawl_all":
        await crawl_command(update, context, crawl_mode_override="all_sites")
        await show_main_menu(update, edit=True)
        return
    if data == "action_crawl_missing":
        await crawl_command(update, context, crawl_mode_override="missing_only")
        await show_main_menu(update, edit=True)
        return
    if data == "action_list_summary":
        await list_command(update, context, scope_override="summary")
        await show_main_menu(update, edit=True)
        return
    if data == "action_list_completed":
        await list_command(update, context, scope_override="completed")
        await show_main_menu(update, edit=True)
        return
    if data == "action_list_genres":
        await list_command(update, context, scope_override="genres")
        await show_main_menu(update, edit=True)
        return
    if data == "action_stats_health":
        await stats_command(update, context, scope_override="health")
        await show_main_menu(update, edit=True)
        return
    if data == "action_stats_disk":
        await stats_command(update, context, scope_override="disk_usage")
        await show_main_menu(update, edit=True)
        return
    if data == "action_stats_top_genres":
        await stats_command(update, context, scope_override="top_genres")
        await show_main_menu(update, edit=True)
        return
    if data == "action_stats_longest":
        await stats_command(update, context, scope_override="longest_stories")
        await show_main_menu(update, edit=True)
        return

    if data == "input_crawl_story":
        await prompt_for_input(update, context, "crawl_story", "🔗 Gửi URL của truyện bạn muốn crawl.")
        return
    if data == "input_crawl_site":
        supported_sites = ", ".join(BASE_URLS.keys())
        await prompt_for_input(
            update,
            context,
            "crawl_site",
            f"🏠 Nhập site key cần crawl (ví dụ: {supported_sites}).",
        )
        return
    if data == "input_retry_failed":
        await prompt_for_input(update, context, "retry_failed", "🔄 Nhập site key cần retry (ví dụ: xtruyen).")
        return
    if data == "input_get_logs":
        await prompt_for_input(
            update,
            context,
            "get_logs",
            "🪵 Nhập số dòng log muốn xem (để trống sử dụng mặc định 50).",
        )
        return


async def handle_user_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Processes free-text replies when the bot is waiting for more details."""
    message = update.effective_message
    if not message or not message.text:
        return

    pending_action = context.user_data.get(PENDING_ACTION_KEY)
    if not pending_action:
        return

    text = message.text.strip()
    if text.lower() in {"/cancel", "cancel", "hủy", "huy"}:
        context.user_data.pop(PENDING_ACTION_KEY, None)
        await message.reply_text("Đã hủy thao tác. Bạn có thể chọn lại trong menu.")
        await show_main_menu(update)
        return

    if not text:
        await message.reply_text("⚠️ Vui lòng nhập dữ liệu hợp lệ hoặc gõ /cancel để hủy.")
        return

    context.user_data.pop(PENDING_ACTION_KEY, None)

    if pending_action == "crawl_story":
        await crawl_story_command(update, context, story_url_override=text)
    elif pending_action == "crawl_site":
        site_key = text.split()[0].lower()
        await crawl_site_command(update, context, site_key_override=site_key)
    elif pending_action == "retry_failed":
        site_key = text.split()[0].lower()
        await retry_failed_command(update, context, site_key_override=site_key)
    elif pending_action == "get_logs":
        num_lines = int(text) if text.isdigit() else None
        await get_logs_command(update, context, num_lines_override=num_lines)
    else:
        await message.reply_text("❔ Không nhận diện được thao tác. Hãy thử lại từ menu.")


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Allows users to cancel any pending interactive action."""
    message = update.effective_message
    had_action = context.user_data.pop(PENDING_ACTION_KEY, None)
    if message:
        if had_action:
            await message.reply_text("Đã hủy thao tác hiện tại.")
        else:
            await message.reply_text("Không có thao tác nào cần hủy.")
    await show_main_menu(update)

# --- Command Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a welcome message and displays the quick action menu."""
    user = update.effective_user
    message = update.effective_message
    if message:
        help_text = (
            f"👋 Chào {user.first_name}, mình là Bot Crawler đây!\n\n"
            "Bạn có thể chọn nhanh chức năng trong menu bên dưới hoặc tiếp tục sử dụng các lệnh quen thuộc:\n"
            "`/build`, `/crawl`, `/status`, `/crawl_story`, `/crawl_site`, `/check_missing`, `/retry_failed`, `/get_logs`, `/list`, `/stats`.\n\n"
            "<b>Mẹo:</b> Menu sẽ hướng dẫn bạn nhập các thông tin cần thiết chỉ với vài bước chạm."
        )
        await message.reply_html(help_text)

    await show_main_menu(update)


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the quick action menu on demand."""
    await show_main_menu(update)

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Checks the status of the crawler system."""
    message = update.effective_message
    if message:
        await message.reply_text("✅ Bot is running and listening for commands.")

async def build_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Builds and pushes the Docker image to Docker Hub."""
    message = update.effective_message
    if message:
        await message.reply_text("⏳ Bắt đầu quá trình build và push image... Logs sẽ được gửi ngay sau đây.")

    command = "docker compose build && docker compose push"

    try:
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT  # Redirect stderr to stdout
        )

        output_chunk = ""
        last_sent_time = time.time()

        # Stream the output
        if process.stdout:
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                
                decoded_line = line.decode('utf-8', errors='ignore')
                output_chunk += decoded_line
                
                # Send in chunks of text or every 2 seconds
                if message and (len(output_chunk) > 3500 or (time.time() - last_sent_time > 2 and output_chunk)):
                    await message.reply_html(f"<pre>{output_chunk}</pre>")
                    output_chunk = ""
                    last_sent_time = time.time()

        # Send any remaining output
        if message and output_chunk:
            await message.reply_html(f"<pre>{output_chunk}</pre>")

        await process.wait()

        if message:
            if process.returncode == 0:
                await message.reply_text("✅ Build và push image thành công!")
            else:
                await message.reply_text(f"❌ Build và push image thất bại! (Exit code: {process.returncode})")

    except Exception as e:
        logger.error(f"[Bot] Lỗi khi thực thi lệnh build: {e}")
        if message:
            await message.reply_text(f"❌ Đã xảy ra lỗi nghiêm trọng khi chạy lệnh build: {e}")

async def crawl_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    crawl_mode_override: str | None = None,
) -> None:
    """Triggers a global crawl job."""
    message = update.effective_message
    args = context.args
    if crawl_mode_override is not None:
        args = [crawl_mode_override]
    if not args:
        if message:
            await message.reply_text(
                "⚠️ Vui lòng cung cấp chế độ crawl.\nVí dụ: `/crawl all_sites` hoặc `/crawl missing_only`"
            )
        return

    crawl_mode = args[0]
    job_type = ""
    
    # Determine job type based on crawl mode
    if crawl_mode in ["all_sites", "full", "genres_only"]:
        job_type = "all_sites"
    elif crawl_mode in ["missing_only", "missing"]:
        job_type = "missing_check"
    else:
        if message:
            await message.reply_text(f"❌ Chế độ crawl '{crawl_mode}' không hợp lệ.")
        return

    job = {"type": job_type, "crawl_mode": crawl_mode}
    success = await send_kafka_job(job)
    if message:
        if success:
            await message.reply_text(f"✅ Đã đưa job `{job_type}` với mode `{crawl_mode}` vào hàng đợi.")
        else:
            await message.reply_text("❌ Gửi job vào Kafka thất bại. Vui lòng kiểm tra logs.")

async def list_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    scope_override: str | None = None,
    filters_override: dict | None = None,
) -> None:
    """Lists and filters stories based on various criteria."""
    message = update.effective_message
    args = context.args
    if scope_override is not None:
        args = [scope_override]
    if not args:
        if message:
            await message.reply_text(
                "Vui lòng cung cấp scope. Ví dụ: `/list completed`, `/list all`, `/list summary`"
            )
        return

    if message:
        await message.reply_text("Đang quét và phân tích thư mục... việc này có thể mất vài giây.")

    scope = args[0].lower()
    filters = filters_override or {
        'genre': None,
        'min_chapters': None,
        'max_chapters': None
    }

    if filters_override is None:
        try:
            for i in range(1, len(args), 2):
                if args[i].startswith('--'):
                    key = args[i][2:]
                    if key in filters and i + 1 < len(args):
                        if key.endswith('_chapters'):
                            filters[key] = int(args[i+1])
                        else:
                            filters[key] = args[i+1]
        except (ValueError, IndexError):
            if message:
                await message.reply_text("❌ Lỗi cú pháp filter. Ví dụ: `--min-chapters 100`")
            return

    stories = get_all_stories()

    if scope == 'summary':
        total_stories = len(stories)
        completed_stories = sum(1 for s in stories if s['status'] == 'completed')
        ongoing_stories = total_stories - completed_stories
        genres = set(s['genre'] for s in stories if s['genre'] != 'Unknown')
        summary_text = (
            f"<b>📊 Thống kê tổng quan:</b>\n"
            f"- Tổng số truyện: {total_stories}\n"
            f"- Đã hoàn thành: {completed_stories}\n"
            f"- Đang theo dõi: {ongoing_stories}\n"
            f"- Số lượng thể loại: {len(genres)}"
        )
        if message:
            await message.reply_html(summary_text)
        return

    if scope == 'genres':
        genres = sorted(list(set(s['genre'] for s in stories if s['genre'] != 'Unknown')))
        genre_text = "<b>📚 Danh sách các thể loại:</b>\n\n" + "\n".join(f"- {g}" for g in genres)
        await send_in_chunks(update, genre_text)
        return

    if scope == 'completed':
        filtered_stories = [s for s in stories if s['status'] == 'completed']
    elif scope == 'all':
        filtered_stories = stories
    else:
        if message:
            await message.reply_text(
                f"Scope không hợp lệ: `{scope}`. Dùng `completed`, `all`, `summary`, hoặc `genres`."
            )
        return

    if filters['genre']:
        filtered_stories = [s for s in filtered_stories if s['genre'] and filters['genre'].lower() in s['genre'].lower()]
    if filters['min_chapters'] is not None:
        filtered_stories = [s for s in filtered_stories if s['total_chapters'] >= filters['min_chapters']]
    if filters['max_chapters'] is not None:
        filtered_stories = [s for s in filtered_stories if s['total_chapters'] <= filters['max_chapters']]

    if not filtered_stories:
        if message:
            await message.reply_text("Không tìm thấy truyện nào khớp với tiêu chí của bạn.")
        return

    output_lines = [f"🔎 Tìm thấy {len(filtered_stories)} truyện:"]
    for story in filtered_stories:
        progress = f"{story['crawled_chapters']}/{story['total_chapters']}"
        line = f"- <b>{story['title']}</b> ({story['status']}) [{story['genre']}] - {progress}"
        output_lines.append(line)
    
    await send_in_chunks(update, "\n".join(output_lines))

async def stats_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    scope_override: str | None = None,
) -> None:
    """Provides detailed statistics about the crawler system."""
    message = update.effective_message
    args = context.args
    if scope_override is not None:
        args = [scope_override]
    if not args:
        if message:
            await message.reply_text("Vui lòng cung cấp scope. Ví dụ: `/stats health`, `/stats disk_usage`")
        return

    scope = args[0].lower()
    if message:
        await message.reply_text(f"Đang tính toán thống kê cho `{scope}`...")

    stories = get_all_stories()

    if scope == 'health':
        health_stats = get_health_stats(stories)
        skipped_stories = health_stats['skipped_stories']
        
        text = f"<b>❤️ Thống kê sức khỏe hệ thống:</b>\n"
        text += f"- Số truyện bị skip: {len(skipped_stories)}\n"
        text += f"- Tổng số chương lỗi: {health_stats['total_dead_chapters']}\n"
        
        if skipped_stories:
            text += "\n<b>Truyện bị skip:</b>\n"
            for s in skipped_stories:
                text += f"- {s['title']} (Lý do: {s['skip_reason']})\n"
        
        if health_stats['stories_with_dead_chapters']:
            text += "\n<b>Truyện có chương lỗi:</b>\n"
            for s in health_stats['stories_with_dead_chapters']:
                text += f"- {s['title']} ({s['dead_count']} chương lỗi)\n"
        await send_in_chunks(update, text)

    elif scope == 'disk_usage':
        total_size_bytes = get_disk_usage(DATA_FOLDER) + get_disk_usage(COMPLETED_FOLDER)
        total_size_gb = total_size_bytes / (1024**3)
        text = f"<b>💾 Thống kê dung lượng:</b>\n"
        text += f"- Tổng dung lượng: {total_size_gb:.2f} GB"
        if message:
            await message.reply_html(text)

    elif scope == 'top_genres':
        count = 5
        if len(args) > 1 and args[1].isdigit():
            count = int(args[1])
        genre_counts = Counter(s['genre'] for s in stories if s['genre'] != 'Unknown')
        top_genres = genre_counts.most_common(count)
        text = f"<b>🏆 Top {len(top_genres)} thể loại có nhiều truyện nhất:</b>\n"
        for i, (genre, num) in enumerate(top_genres):
            text += f"{i+1}. {genre}: {num} truyện\n"
        if message:
            await message.reply_html(text)

    elif scope == 'longest_stories':
        count = 10
        if len(args) > 1 and args[1].isdigit():
            count = int(args[1])
        
        # Sort by total_chapters, descending
        longest = sorted(stories, key=lambda s: s.get('total_chapters', 0), reverse=True)
        top_longest = longest[:count]
        
        text = f"<b>📖 Top {len(top_longest)} truyện dài nhất:</b>\n"
        for i, s in enumerate(top_longest):
            text += f"{i+1}. {s['title']} - {s['total_chapters']} chương\n"
        await send_in_chunks(update, text)

    else:
        if message:
            await message.reply_text(
                f"Scope không hợp lệ: `{scope}`. Dùng `health`, `disk_usage`, `top_genres`, `longest_stories`."
            )


async def crawl_story_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    story_url_override: str | None = None,
) -> None:
    """Triggers a crawl job for a single story URL."""
    message = update.effective_message
    story_url = story_url_override
    if story_url is None:
        if not context.args:
            if message:
                await message.reply_text(
                    "⚠️ Vui lòng cung cấp URL của truyện.\nVí dụ: `/crawl_story https://xtruyen.vn/truyen/...`"
                )
            return
        story_url = context.args[0]

    if not story_url:
        if message:
            await message.reply_text("⚠️ URL không hợp lệ.")
        return
    job = {"type": "single_story", "url": story_url}

    success = await send_kafka_job(job)
    if message:
        if success:
            await message.reply_text(f"✅ Đã đưa job `crawl_story` cho URL: {story_url} vào hàng đợi.")
        else:
            await message.reply_text("❌ Gửi job vào Kafka thất bại. Vui lòng kiểm tra logs.")

async def crawl_site_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    site_key_override: str | None = None,
) -> None:
    """Triggers a crawl job for a full site."""
    message = update.effective_message
    site_key = site_key_override
    if site_key is None:
        if not context.args:
            if message:
                await message.reply_text(
                    f"⚠️ Vui lòng cung cấp site key.\nVí dụ: `/crawl_site xtruyen`\nCác site được hỗ trợ: {', '.join(BASE_URLS.keys())}"
                )
            return
        site_key = context.args[0]

    if site_key not in BASE_URLS:
        if message:
            await message.reply_text(
                f"❌ Site key '{site_key}' không hợp lệ.\nCác site được hỗ trợ: {', '.join(BASE_URLS.keys())}"
            )
        return

    job = {"type": "full_site", "site_key": site_key}
    success = await send_kafka_job(job)
    if message:
        if success:
            await message.reply_text(f"✅ Đã đưa job `crawl_site` cho site: `{site_key}` vào hàng đợi.")
        else:
            await message.reply_text("❌ Gửi job vào Kafka thất bại. Vui lòng kiểm tra logs.")

async def check_missing_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Triggers a job to check for missing chapters."""
    message = update.effective_message
    job = {"type": "missing_check"}
    success = await send_kafka_job(job)
    if message:
        if success:
            await message.reply_text("✅ Đã đưa job `check_missing` vào hàng đợi.")
        else:
            await message.reply_text("❌ Gửi job vào Kafka thất bại. Vui lòng kiểm tra logs.")

async def retry_failed_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    site_key_override: str | None = None,
) -> None:
    """Triggers a job to retry failed genres/stories."""
    message = update.effective_message
    site_key = site_key_override
    if site_key is None:
        if not context.args:
            if message:
                await message.reply_text(f"⚠️ Vui lòng cung cấp site key để retry.\nVí dụ: `/retry_failed xtruyen`")
            return
        site_key = context.args[0]

    if not site_key:
        return
    job = {"type": "retry_failed_genres", "site_key": site_key}
    success = await send_kafka_job(job)
    if message:
        if success:
            await message.reply_text(f"✅ Đã đưa job `retry_failed` cho site `{site_key}` vào hàng đợi.")
        else:
            await message.reply_text("❌ Gửi job vào Kafka thất bại. Vui lòng kiểm tra logs.")

async def get_logs_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    num_lines_override: int | None = None,
) -> None:
    """Retrieves the last N lines of the latest log file."""
    message = update.effective_message
    try:
        num_lines = num_lines_override if num_lines_override is not None else 50
        if num_lines_override is None and context.args and context.args[0].isdigit():
            num_lines = int(context.args[0])

        log_files = glob.glob(os.path.join(LOG_FOLDER, '*.log'))
        if not log_files:
            if message:
                await message.reply_text("Không tìm thấy file log nào.")
            return

        latest_log_file = max(log_files, key=os.path.getctime)

        with open(latest_log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            last_n_lines = lines[-num_lines:]

        log_content = "".join(last_n_lines)

        if not log_content:
            if message:
                await message.reply_text(f"File log `{os.path.basename(latest_log_file)}` trống.")
            return

        await send_in_chunks(update, log_content)

    except Exception as e:
        logger.error(f"[Bot] Lỗi khi đọc logs: {e}")
        if message:
            await message.reply_text(f"❌ Đã xảy ra lỗi khi cố gắng đọc file log: {e}")

# --- Bot Setup ---

async def main_bot():
    """Starts the Telegram bot and registers command handlers."""
    if not TELEGRAM_BOT_TOKEN:
        logger.error("[Bot] TELEGRAM_BOT_TOKEN chưa được cấu hình. Bot không thể khởi động.")
        return

    logger.info("[Bot] Đang khởi tạo bot...")
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Register command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", start_command))
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("build", build_command))
    application.add_handler(CommandHandler("crawl", crawl_command))
    application.add_handler(CommandHandler("list", list_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("crawl_story", crawl_story_command))
    application.add_handler(CommandHandler("crawl_site", crawl_site_command))
    application.add_handler(CommandHandler("check_missing", check_missing_command))
    application.add_handler(CommandHandler("retry_failed", retry_failed_command))
    application.add_handler(CommandHandler("get_logs", get_logs_command))
    application.add_handler(CallbackQueryHandler(menu_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_user_input))

    logger.info("[Bot] Bot đang chạy và lắng nghe lệnh...")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_stop_signal() -> None:
        if not stop_event.is_set():
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_stop_signal)
        except NotImplementedError:
            # Signal handlers may not be available on all platforms (e.g., Windows)
            pass

    try:
        async with application:
            await application.start()

            if application.updater:
                await application.updater.start_polling()
            else:
                logger.warning("[Bot] Không thể khởi động polling vì không có updater. Vui lòng kiểm tra cấu hình bot.")

            logger.info("[Bot] Bot đã khởi động thành công và đang chạy.")

            await stop_event.wait()
    except asyncio.CancelledError:
        logger.info("[Bot] Vòng lặp bot đã bị hủy. Đang tiến hành tắt bot...")
    except (KeyboardInterrupt, SystemExit):
        logger.info("[Bot] Nhận được tín hiệu dừng, đang tắt bot...")
    finally:
        if application.updater:
            await application.updater.stop()
        await stop_kafka_producer()
        logger.info("[Bot] Bot đã dừng hoàn toàn.")


def run_bot() -> None:
    """Entry point for starting the Telegram bot."""
    try:
        asyncio.run(main_bot())
    except RuntimeError as exc:
        # Handle the "event loop is already running" scenario gracefully.
        if "event loop is already running" in str(exc):
            logger.error("[Bot] Không thể khởi động bot vì vòng lặp asyncio đã chạy sẵn."
                         " Hãy đảm bảo bot được khởi chạy như một tiến trình riêng biệt.")
        else:
            raise


if __name__ == "__main__":
    run_bot()
