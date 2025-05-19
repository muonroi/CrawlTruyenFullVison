# Crawl Truyện Full - Python Async Crawler

An advanced story crawling project, supporting async operations, multiple sources, multiple genres, state backup, chapter retry, batching, automatic proxy management, and Telegram notifications.

---

**🌐 Choose Your Language / Chọn Ngôn Ngữ Của Bạn:**

-   [Tiếng Việt (Vietnamese)](README.vi.md)
-   [English (English)](README.en.md)
---

This project helps you crawl stories from various sources efficiently. For detailed information, please select your preferred language above.

Crawl Full Stories - Python Async Crawler
An advanced story crawling project that supports async, multiple sources, multiple genres, backup state, retry failed chapters, batch processing, automatic proxy management, and sends notifications via Telegram.
🚀 Key Features
- Async & Multi-batch: Crawl multiple chapters/stories/genres in parallel, automatically optimize batch processing.
- Backup & Retry: Automatically save crawl state, periodic backups, and automatically retry failed chapters/genres.
- Advanced proxy management: Supports proxy rotation, auto removal of faulty proxies, warning when proxies run out via Telegram.
- Send Telegram notifications: Report completion status or proxy error warnings to Telegram.
- Customized site parsers: Supports multiple sources such as truyenfull, metruyenfull..., easy to extend to new domains.
- Intelligent content cleaning: Filter ads, redundant lines, and implement anti-spam according to easily expandable patterns.
- Standard metadata storage: Complete metadata for each story, automatic backup & validation.
- Recover missed chapters/genres: Automatically detect and crawl additional chapters/stories. at_id
# ... other variables (see config.py file or related configuration file)
3. Prepare the proxy:
o Add the list of proxies to the proxies/ directory (for example: proxies/proxies.txt).
o One proxy per line, supports formats: IP:PORT or USER:PASS@IP:PORT.
4. Customize the spam and advertising filter patterns:
o Edit the config/blacklist_patterns.txt file (or corresponding configuration file) to define detection patterns and remove unwanted lines from the chapter content.
🏃‍♂️ How to run the crawler
By default, the crawler can be configured to run a specific site. To specify the site you want to crawl (for example, truyenfull or metruyenfull):
python main.py truyenfull
# or
python main.py metruyenfull
(Note: The exact command may vary depending on how you design main.py to accept input parameters.)
🛠️ Some important files/modules
main.py: Orchestrates the entire crawling process (from category → story → chapter), managing state, batch, retry.
adapters/: Defines each Adapter for each site (for example: error to retry later  
⚡ Expand - customize additional domain  
Create a new Adapter: Create a new Adapter class that inherits from BaseSiteAdapter (or a similar base class).  
Implement methods: Override necessary methods such as get_genres, get_story_list_by_genre, get_story_details, get_chapter_list, get_chapter_content, etc.  
Register Adapter: Update in factory.py (or a similar mechanism) so that the system can recognize and use the new Adapter.  
⏱️ Note when running in production  
- It is advisable to use a server or VPS with a "clean" IP or high-quality proxy sources.  
Use crontab (Linux) or Task Scheduler (Windows) to run automatically on a regular basis, or run in screen/tmux to avoid losing state during SSH disconnections.  
Regularly monitor log files and backup directories.  
Customize parameters such as batch_size, buffer_size, request_delay to suit server resources and the policies of the source websites.  
Contact & Support  
Author: muonroi  
Contact Telegram (for proxy alerts, information)
