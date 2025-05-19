# Crawl Truyện Full - Python Async Crawler

Một project crawl truyện nâng cao, hỗ trợ async, đa nguồn, đa thể loại, backup trạng thái, retry chương lỗi, phân batch, quản lý proxy tự động, gửi thông báo Telegram.

## 🚀 Tính năng nổi bật

-   **Async & Multi-batch:** Crawl song song hàng loạt chương/truyện/thể loại, tự động phân batch tối ưu.
-   **Backup & Retry:** Tự động lưu trạng thái crawl, backup định kỳ, tự động retry các chương/thể loại bị lỗi.
-   **Quản lý proxy nâng cao:** Hỗ trợ rotate proxy, auto remove proxy lỗi, cảnh báo hết proxy qua Telegram.
-   **Gửi thông báo Telegram:** Báo trạng thái hoàn thành hoặc cảnh báo lỗi proxy đến Telegram.
-   **Parser tuỳ biến từng site:** Hỗ trợ nhiều nguồn như truyenfull, metruyenfull..., dễ mở rộng domain mới.
-   **Clean nội dung thông minh:** Lọc quảng cáo, dòng thừa, anti-spam theo pattern dễ mở rộng.
-   **Lưu metadata chuẩn:** Metadata đầy đủ cho từng truyện, backup & validate tự động.
-   **Phục hồi chương/thể loại bị miss:** Tự động detect và crawl bù chương/truyện/thể loại thiếu hoặc lỗi.

## 🗂️ Cấu trúc thư mục chính
```json
├── adapters/              # Adapter cho từng site nguồn (truyenfull, metruyenfull,...)
├── analyze/               # Các hàm phân tích HTML, extract nội dung
├── config/                # Các file cấu hình, biến môi trường, proxy, pattern blacklist
├── utils/                 # Các tiện ích chung: logger, io, async, notifier,...
├── main.py                # Entry point chính để chạy crawler
├── requirements.txt       # Thư viện cần thiết
└── README.md              # Tài liệu này
```

## ⚙️ Hướng dẫn cài đặt

1.  **Clone project và cài đặt thư viện:**
    ```bash
    git clone <URL_REPO_CUA_BAN> # Thay <URL_REPO_CUA_BAN> bằng URL thực tế
    cd CrawlTruyenFullVison # Hoặc tên thư mục project của bạn
    pip install -r requirements.txt
    ```

2.  **Tạo file `.env` để cấu hình (tham khảo file `config/config.py` hoặc file `.env.example` nếu có):**
    ```env
    REQUEST_DELAY=4
    USE_PROXY=True
    PROXIES_FOLDER=proxies
    TELEGRAM_BOT_TOKEN=your_telegram_bot_token
    TELEGRAM_CHAT_ID=your_telegram_chat_id
    # ... các biến khác (xem file config.py hoặc file cấu hình liên quan)
    ```

3.  **Chuẩn bị proxy:**
    * Thêm danh sách proxy vào thư mục `proxies/` (ví dụ: `proxies/proxies.txt`).
    * Mỗi proxy một dòng, hỗ trợ các định dạng: `IP:PORT` hoặc `USER:PASS@IP:PORT`.

4.  **Tùy chỉnh các pattern lọc spam, quảng cáo:**
    * Sửa file `config/blacklist_patterns.txt` (hoặc file cấu hình tương ứng) để định nghĩa các mẫu nhận diện và loại bỏ các dòng không mong muốn khỏi nội dung chương truyện.

## 🏃‍♂️ Cách chạy crawler

Mặc định, crawler có thể được cấu hình để chạy một site cụ thể. Để chỉ định site muốn crawl (ví dụ `truyenfull` hoặc `metruyenfull`):

```bash
python main.py truyenfull
# hoặc
python main.py metruyenfull
```
### (Lưu ý: Câu lệnh chính xác có thể thay đổi tùy theo cách bạn thiết kế main.py để nhận tham số đầu vào.)

## 🛠️ Một số file/module quan trọng

**main.py**: *Điều phối toàn bộ quá trình crawl (từ thể loại → truyện → chương), quản lý trạng thái, batch, retry.*

**adapters/**: *Định nghĩa từng Adapter cho mỗi site (ví dụ: TruyenFullAdapter, MeTruyenFullAdapter), giúp dễ dàng mở rộng và quản lý logic riêng cho từng domain.*

**scraper.py (hoặc module tương tự)**: *Quản lý việc gửi request HTTP, fake User-Agent, luân chuyển proxy, xử lý các lỗi thường gặp (như 403), retry và tự động loại bỏ proxy lỗi*

**utils/logger.py**: *Ghi log chi tiết quá trình hoạt động ra file (ví dụ: crawler.log) và hiển thị trên console.*

**utils/notifier.py**: *Gửi thông báo qua Telegram khi hoàn tất quá trình crawl, hoặc khi có cảnh báo quan trọng (ví dụ: hết proxy khả dụng).*

**utils/cleaner.py, analyze/html_parser.py (hoặc các module tương tự)**: *Chịu trách nhiệm làm sạch nội dung chương truyện, loại bỏ quảng cáo, các đoạn spam, đảm bảo nội dung thu được là chuẩn nhất.*

**utils/meta_utils.py**: *Quản lý việc lưu trữ metadata của truyện, backup trạng thái crawl, và phục hồi trạng thái khi cần.

**utils/chapter_utils.py**: *Chứa các hàm xử lý việc lưu trữ chương truyện (có thể là async), kiểm tra hash để tránh lưu trùng lặp, và quản lý hàng đợi các chương bị lỗi để retry sau*

**⚡ Mở rộng - tuỳ chỉnh thêm domain**
**Tạo Adapter mới**: *Tạo một class Adapter mới kế thừa từ BaseSiteAdapter (hoặc một class cơ sở tương tự).*

**Implement các phương thức**: *Viết lại (override) các phương thức cần thiết như get_genres, get_story_list_by_genre, get_story_details, get_chapter_list, get_chapter_content, v.v.*

**Đăng ký Adapter**: *Cập nhật vào factory.py (hoặc cơ chế tương tự) để hệ thống có thể nhận diện và sử dụng Adapter mới.*

**⏱️ Lưu ý khi chạy thật/production**

-   *Nên sử dụng server, VPS có IP "sạch" hoặc sử dụng nguồn proxy chất lượng cao.
    Sử dụng crontab (Linux) hoặc Task Scheduler (Windows) để tự động chạy định kỳ, hoặc chạy trong screen/tmux để tránh mất trạng thái khi mất kết nối SSH.
    Thường xuyên theo dõi file log và thư mục backup.
    Tùy chỉnh các thông số như batch_size, buffer_size, request_delay cho phù hợp với tài  nguyên của server và chính sách của các website nguồn.*

**Liên hệ & Hỗ trợ**
**Author**: `muonroi`

*Liên hệ Telegram (cho cảnh báo proxy, thông báo crawl xong): Thiết lập TELEGRAM_BOT_TOKEN và TELEGRAM_CHAT_ID trong file .env.*
