
import asyncio
import os
import glob
import time
import signal
from collections import Counter
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from utils.logger import logger
from utils.kafka_producer import send_kafka_job, stop_kafka_producer
from config.config import TELEGRAM_BOT_TOKEN, LOG_FOLDER, BASE_URLS, DATA_FOLDER, COMPLETED_FOLDER
from utils.story_analyzer import get_all_stories, get_health_stats, get_disk_usage

# --- Helper for sending long messages ---
async def send_in_chunks(update: Update, text: str, max_chars: int = 4000):
    """Sends a long message in chunks to avoid hitting Telegram's limit."""
    if not text:
        await update.message.reply_text("Không có dữ liệu để hiển thị.")
        return
    for i in range(0, len(text), max_chars):
        chunk = text[i:i+max_chars]
        await update.message.reply_html(f"<pre>{chunk}</pre>")

# --- Command Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a welcome message and lists available commands."""
    user = update.effective_user
    help_text = (
        f"👋 Chào {user.first_name}, mình là Bot Crawler đây!\n\n"
        "Dưới đây là các lệnh mình hỗ trợ:\n"
        "`/build` - Build và push image mới nhất lên Docker Hub.\n"
        "`/crawl <mode>` - Bắt đầu một phiên crawl (ví dụ: `/crawl all_sites`).\n"
        "`/status` - Kiểm tra trạng thái của hệ thống.\n"
        "`/crawl_story <URL>` - Crawl một truyện từ URL.\n"
        "`/crawl_site <site_key>` - Crawl toàn bộ một trang.\n"
        "`/check_missing` - Chạy tác vụ kiểm tra chương còn thiếu.\n"
        "`/retry_failed <site_key>` - Thử lại các job đã lỗi.\n"
        "`/get_logs [số_dòng]` - Lấy log mới nhất.\n"
        "`/list <scope> [--filter value]` - Liệt kê và lọc truyện.\n"
        "`/stats <scope>` - Xem các thống kê chi tiết.\n"
        "`/help` - Hiển thị lại danh sách lệnh này.\n\n"
        "<b>Ví dụ lệnh /list:</b>\n"
        "  `/list completed --genre \'Tiên Hiệp\'`\n"
        "<b>Ví dụ lệnh /stats:</b>\n"
        "  `/stats health`\n"
        "  `/stats top_genres 10`\n"
    )
    await update.message.reply_html(help_text)

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Checks the status of the crawler system."""
    await update.message.reply_text("✅ Bot is running and listening for commands.")

async def build_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Builds and pushes the Docker image to Docker Hub."""
    await update.message.reply_text("⏳ Bắt đầu quá trình build và push image... Logs sẽ được gửi ngay sau đây.")
    
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
                if len(output_chunk) > 3500 or (time.time() - last_sent_time > 2 and output_chunk):
                    await update.message.reply_html(f"<pre>{output_chunk}</pre>")
                    output_chunk = ""
                    last_sent_time = time.time()

        # Send any remaining output
        if output_chunk:
            await update.message.reply_html(f"<pre>{output_chunk}</pre>")

        await process.wait()

        if process.returncode == 0:
            await update.message.reply_text("✅ Build và push image thành công!")
        else:
            await update.message.reply_text(f"❌ Build và push image thất bại! (Exit code: {process.returncode})")

    except Exception as e:
        logger.error(f"[Bot] Lỗi khi thực thi lệnh build: {e}")
        await update.message.reply_text(f"❌ Đã xảy ra lỗi nghiêm trọng khi chạy lệnh build: {e}")

async def crawl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Triggers a global crawl job."""
    args = context.args
    if not args:
        await update.message.reply_text("⚠️ Vui lòng cung cấp chế độ crawl.\nVí dụ: `/crawl all_sites` hoặc `/crawl missing_only`")
        return

    crawl_mode = args[0]
    job_type = ""
    
    # Determine job type based on crawl mode
    if crawl_mode in ["all_sites", "full", "genres_only"]:
        job_type = "all_sites"
    elif crawl_mode in ["missing_only", "missing"]:
        job_type = "missing_check"
    else:
        await update.message.reply_text(f"❌ Chế độ crawl '{crawl_mode}' không hợp lệ.")
        return

    job = {"type": job_type, "crawl_mode": crawl_mode}
    success = await send_kafka_job(job)
    if success:
        await update.message.reply_text(f"✅ Đã đưa job `{job_type}` với mode `{crawl_mode}` vào hàng đợi.")
    else:
        await update.message.reply_text("❌ Gửi job vào Kafka thất bại. Vui lòng kiểm tra logs.")

async def list_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lists and filters stories based on various criteria."""
    args = context.args
    if not args:
        await update.message.reply_text("Vui lòng cung cấp scope. Ví dụ: `/list completed`, `/list all`, `/list summary`")
        return

    await update.message.reply_text("Đang quét và phân tích thư mục... việc này có thể mất vài giây.")

    scope = args[0].lower()
    filters = {
        'genre': None,
        'min_chapters': None,
        'max_chapters': None
    }

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
        await update.message.reply_text("❌ Lỗi cú pháp filter. Ví dụ: `--min-chapters 100`")
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
        await update.message.reply_html(summary_text)
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
        await update.message.reply_text(f"Scope không hợp lệ: `{scope}`. Dùng `completed`, `all`, `summary`, hoặc `genres`.")
        return

    if filters['genre']:
        filtered_stories = [s for s in filtered_stories if s['genre'] and filters['genre'].lower() in s['genre'].lower()]
    if filters['min_chapters'] is not None:
        filtered_stories = [s for s in filtered_stories if s['total_chapters'] >= filters['min_chapters']]
    if filters['max_chapters'] is not None:
        filtered_stories = [s for s in filtered_stories if s['total_chapters'] <= filters['max_chapters']]

    if not filtered_stories:
        await update.message.reply_text("Không tìm thấy truyện nào khớp với tiêu chí của bạn.")
        return

    output_lines = [f"🔎 Tìm thấy {len(filtered_stories)} truyện:"]
    for story in filtered_stories:
        progress = f"{story['crawled_chapters']}/{story['total_chapters']}"
        line = f"- <b>{story['title']}</b> ({story['status']}) [{story['genre']}] - {progress}"
        output_lines.append(line)
    
    await send_in_chunks(update, "\n".join(output_lines))

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Provides detailed statistics about the crawler system."""
    args = context.args
    if not args:
        await update.message.reply_text("Vui lòng cung cấp scope. Ví dụ: `/stats health`, `/stats disk_usage`")
        return

    scope = args[0].lower()
    await update.message.reply_text(f"Đang tính toán thống kê cho `{scope}`...")

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
        await update.message.reply_html(text)

    elif scope == 'top_genres':
        count = 5
        if len(args) > 1 and args[1].isdigit():
            count = int(args[1])
        genre_counts = Counter(s['genre'] for s in stories if s['genre'] != 'Unknown')
        top_genres = genre_counts.most_common(count)
        text = f"<b>🏆 Top {len(top_genres)} thể loại có nhiều truyện nhất:</b>\n"
        for i, (genre, num) in enumerate(top_genres):
            text += f"{i+1}. {genre}: {num} truyện\n"
        await update.message.reply_html(text)

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
        await update.message.reply_text(f"Scope không hợp lệ: `{scope}`. Dùng `health`, `disk_usage`, `top_genres`, `longest_stories`.")


async def crawl_story_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Triggers a crawl job for a single story URL."""
    if not context.args:
        await update.message.reply_text("⚠️ Vui lòng cung cấp URL của truyện.\nVí dụ: `/crawl_story https://xtruyen.vn/truyen/...`")
        return

    story_url = context.args[0]
    job = {"type": "single_story", "url": story_url}

    success = await send_kafka_job(job)
    if success:
        await update.message.reply_text(f"✅ Đã đưa job `crawl_story` cho URL: {story_url} vào hàng đợi.")
    else:
        await update.message.reply_text("❌ Gửi job vào Kafka thất bại. Vui lòng kiểm tra logs.")

async def crawl_site_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Triggers a crawl job for a full site."""
    if not context.args:
        await update.message.reply_text(f"⚠️ Vui lòng cung cấp site key.\nVí dụ: `/crawl_site xtruyen`\nCác site được hỗ trợ: {', '.join(BASE_URLS.keys())}")
        return

    site_key = context.args[0]
    if site_key not in BASE_URLS:
        await update.message.reply_text(f"❌ Site key '{site_key}' không hợp lệ.\nCác site được hỗ trợ: {', '.join(BASE_URLS.keys())}")
        return

    job = {"type": "full_site", "site_key": site_key}
    success = await send_kafka_job(job)
    if success:
        await update.message.reply_text(f"✅ Đã đưa job `crawl_site` cho site: `{site_key}` vào hàng đợi.")
    else:
        await update.message.reply_text("❌ Gửi job vào Kafka thất bại. Vui lòng kiểm tra logs.")

async def check_missing_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Triggers a job to check for missing chapters."""
    job = {"type": "missing_check"}
    success = await send_kafka_job(job)
    if success:
        await update.message.reply_text("✅ Đã đưa job `check_missing` vào hàng đợi.")
    else:
        await update.message.reply_text("❌ Gửi job vào Kafka thất bại. Vui lòng kiểm tra logs.")

async def retry_failed_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Triggers a job to retry failed genres/stories."""
    if not context.args:
        await update.message.reply_text(f"⚠️ Vui lòng cung cấp site key để retry.\nVí dụ: `/retry_failed xtruyen`")
        return

    site_key = context.args[0]
    job = {"type": "retry_failed_genres", "site_key": site_key}
    success = await send_kafka_job(job)
    if success:
        await update.message.reply_text(f"✅ Đã đưa job `retry_failed` cho site `{site_key}` vào hàng đợi.")
    else:
        await update.message.reply_text("❌ Gửi job vào Kafka thất bại. Vui lòng kiểm tra logs.")

async def get_logs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Retrieves the last N lines of the latest log file."""
    try:
        num_lines = 50
        if context.args and context.args[0].isdigit():
            num_lines = int(context.args[0])

        log_files = glob.glob(os.path.join(LOG_FOLDER, '*.log'))
        if not log_files:
            await update.message.reply_text("Không tìm thấy file log nào.")
            return

        latest_log_file = max(log_files, key=os.path.getctime)
        
        with open(latest_log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            last_n_lines = lines[-num_lines:]

        log_content = "".join(last_n_lines)

        if not log_content:
            await update.message.reply_text(f"File log `{os.path.basename(latest_log_file)}` trống.")
            return

        await send_in_chunks(update, log_content)

    except Exception as e:
        logger.error(f"[Bot] Lỗi khi đọc logs: {e}")
        await update.message.reply_text(f"❌ Đã xảy ra lỗi khi cố gắng đọc file log: {e}")

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
