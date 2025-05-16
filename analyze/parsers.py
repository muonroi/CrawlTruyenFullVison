# analyze/parsers.py
import re
from typing import List, Tuple, Optional, Dict, Any
from bs4 import BeautifulSoup, Comment
from urllib.parse import urljoin, urlparse

# Đảm bảo các import này là chính xác dựa trên cấu trúc file của bạn
from utils.utils import logger, sanitize_filename # Sửa: Sử dụng logger từ utils.utils
from scraper import make_request
from config.config import ( # Sửa: Import từ config.config
    BASE_URL,
    MAX_STORIES_PER_GENRE_PAGE, # Được sử dụng trong get_all_stories_from_genre
    MAX_CHAPTER_PAGES_TO_CRAWL  # Được sử dụng trong get_chapters_from_story
)

# --- GET ALL GENRES ---
def get_all_genres(homepage_url: str) -> List[Dict[str, str]]: # Sửa: Thêm type hint
    """
    Lấy danh sách tất cả các thể loại và URL tương ứng từ trang chủ.
    """
    logger.info(f"Đang lấy danh sách thể loại từ: {homepage_url}") # Sửa: print -> logger.info
    response = make_request(homepage_url)
    if not response or not response.text: # Sửa: Kiểm tra thêm response.text
        logger.error(f"Không nhận được phản hồi hoặc nội dung rỗng từ {homepage_url}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    genres: List[Dict[str, str]] = [] # Sửa: Type hint

    nav_menu = soup.find("ul", class_="control nav navbar-nav")
    if not nav_menu:
        logger.debug("Không tìm thấy menu chính 'ul.control.nav.navbar-nav'. Thử fallback...") # Sửa: print -> logger.debug
        nav_menu = soup.find("div", class_="navbar-collapse")
        if not nav_menu:
            logger.warning("Không tìm thấy container menu nào khả thi để lấy thể loại.") # Sửa: print -> logger.warning
            return []

    genre_dropdown_container = None
    dropdown_toggles = nav_menu.find_all("a", class_=re.compile(r"dropdown-toggle|nav-link"))
    for toggle_link in dropdown_toggles:
        if "Thể loại" in toggle_link.get_text(strip=True):
            parent_li = toggle_link.find_parent("li", class_="dropdown")
            if parent_li:
                genre_dropdown_container = parent_li.find("div", class_=re.compile(r"dropdown-menu"))
                if genre_dropdown_container:
                    logger.debug("Tìm thấy dropdown thể loại qua text 'Thể loại' và parent 'li.dropdown'")
                    break
            sibling_menu = toggle_link.find_next_sibling(class_=re.compile(r"dropdown-menu"))
            if sibling_menu:
                genre_dropdown_container = sibling_menu
                logger.debug("Tìm thấy dropdown thể loại qua text 'Thể loại' và sibling menu.")
                break

    if not genre_dropdown_container:
        logger.debug("Không tìm thấy qua text 'Thể loại'. Thử tìm trực tiếp div dropdown menu...")
        menus = nav_menu.find_all("div", class_=re.compile(r"dropdown-menu|multi-column"))
        for menu_candidate in menus:
            potential_links = menu_candidate.find_all("a", href=re.compile(r"/the-loai/"))
            if potential_links:
                genre_dropdown_container = menu_candidate
                logger.debug(f"Tìm thấy container thể loại tiềm năng qua class: {menu_candidate.get('class')}") # Sửa: print -> logger.debug
                break

    if genre_dropdown_container:
        genre_links = genre_dropdown_container.find_all("a", href=True)
        for link in genre_links:
            genre_name = link.get_text(strip=True)
            genre_url_relative = link.get("href")
            if (genre_name and genre_url_relative and
                ("/the-loai/" in genre_url_relative or "/genres/" in genre_url_relative)): # Logic cũ để kiểm tra link thể loại
                genre_url_absolute = urljoin(homepage_url, genre_url_relative)
                genres.append({"name": genre_name, "url": genre_url_absolute})
    else:
        logger.warning("Không tìm thấy dropdown thể loại sau nhiều lần thử.") # Sửa: print -> logger.warning

    unique_genres: List[Dict[str,str]] = [] # Sửa: Type hint
    seen_urls: set[str] = set() # Sửa: Type hint
    for genre in genres:
        if genre["url"] not in seen_urls:
            unique_genres.append(genre)
            seen_urls.add(genre["url"])
            logger.debug(f"Thêm thể loại: {genre['name']} ({genre['url']})")

    logger.info(f"Tìm thấy {len(unique_genres)} thể loại.") # Sửa: print -> logger.info
    return unique_genres


# --- GET STORIES FROM GENRE PAGE ---
# (Hàm này bạn đã cung cấp trong câu hỏi trước và đã có logger, type hint)
# Đảm bảo hàm này giữ nguyên hoặc được cập nhật nếu cần
def get_stories_from_genre_page(genre_page_url: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    logger.info(f"Đang lấy truyện từ trang thể loại: {genre_page_url}")
    response = make_request(genre_page_url)
    if not response or not response.text:
        logger.error(f"Không nhận được phản hồi hoặc nội dung rỗng từ {genre_page_url}")
        return [], None

    soup = BeautifulSoup(response.text, "html.parser")
    stories: List[Dict[str, Any]] = []
    next_page_url: Optional[str] = None

    list_truyen_container = soup.find("div", class_="list-truyen")
    if not list_truyen_container:
        list_truyen_container = soup.find("div", id="list-page")
        if list_truyen_container:
            list_truyen_container_child = list_truyen_container.find("div", class_="list-truyen")
            if list_truyen_container_child:
                list_truyen_container = list_truyen_container_child

    if not list_truyen_container:
        potential_containers = soup.find_all("div", class_="row")
        for pc in potential_containers:
            if pc.find("h3", class_="truyen-title") or pc.find("div", itemscope=True, itemtype="https://schema.org/Book"):
                list_truyen_container = pc
                if pc.find("h3", class_="truyen-title") and not pc.find_next_sibling("div", class_="row"):
                    if pc.parent and pc.parent.name != "body":
                        list_truyen_container = pc.parent
                logger.debug(f"Sử dụng container truyện dự phòng: <{list_truyen_container.name} class='{list_truyen_container.get('class')}'>")
                break
    
    story_items_source = []
    if not list_truyen_container:
        logger.warning(f"Không tìm thấy container chính của danh sách truyện trên: {genre_page_url}")
        story_items_direct = soup.find_all("div", itemscope=True, itemtype="https://schema.org/Book")
        if story_items_direct:
            story_items_source = story_items_direct
            logger.debug(f"Tìm thấy {len(story_items_source)} item truyện trực tiếp (không qua container).")
        else:
            logger.warning(f"Không tìm thấy item truyện nào trên {genre_page_url} bằng các phương pháp đã thử.")
            return [], None
    else:
        story_items_source = list_truyen_container.find_all("div", class_="row", itemscope=True, itemtype="https://schema.org/Book")
        if not story_items_source:
            logger.debug("Không tìm thấy item truyện với itemscope. Thử selector chung hơn...")
            story_items_source = list_truyen_container.find_all("div", class_=re.compile(r"story-item|item|book-item|comic-item|row"))
            story_items_source = [item for item in story_items_source if item.find("h3", class_="truyen-title")]

    for item in story_items_source:
        story_title = None
        story_url_absolute = None
        author_name = "N/A" # Khởi tạo giá trị mặc định
        image_url = None

        title_tag = item.find("h3", class_="truyen-title")
        if not title_tag:
            title_tag = item.find(["h2", "h4"], class_=re.compile(r"title|name"))

        if title_tag:
            link_tag = title_tag.find("a", href=True)
            if link_tag:
                story_title = link_tag.get_text(strip=True)
                story_url_relative = link_tag["href"]
                story_url_absolute = urljoin(genre_page_url, story_url_relative)

        author_span = item.find("span", class_="author", itemprop="author")
        if author_span:
            author_name_candidate = author_span.get_text(strip=True)
            if author_name_candidate and author_name_candidate.lower() != "n/a" and len(author_name_candidate) >= 2:
                author_name = author_name_candidate
            elif author_span.contents:
                last_content = author_span.contents[-1]
                if isinstance(last_content, str) and last_content.strip():
                    author_name = last_content.strip()
                elif hasattr(last_content, 'get_text'):
                    author_name_from_tag = last_content.get_text(strip=True)
                    if author_name_from_tag : author_name = author_name_from_tag
        
        image_col_div = item.find("div", class_="col-xs-3")
        if image_col_div:
            lazy_img_div = image_col_div.find("div", class_="lazyimg")
            if lazy_img_div:
                image_url_candidate = lazy_img_div.get("data-image") or lazy_img_div.get("data-desk-image")
                if image_url_candidate:
                    if not image_url_candidate.startswith(("http://", "https://")):
                        image_url = urljoin(BASE_URL, image_url_candidate)
                    else:
                        image_url = image_url_candidate

        if story_title and story_url_absolute:
            stories.append({
                "title": story_title, "url": story_url_absolute,
                "author": author_name, "image_url": image_url
            })
            logger.debug(f"  Lấy thông tin: {story_title} - Tác giả: {author_name} - Ảnh: {image_url}")

    pagination = soup.find("ul", class_="pagination")
    if not pagination:
        pagination = soup.find("div", class_=re.compile(r"pagination|wp-pagenavi|page-nav"))

    if pagination:
        active_li_or_span = pagination.find(["li", "span"], class_=re.compile(r"active|current"))
        next_item_tag = None # Đổi tên biến để tránh nhầm lẫn
        if active_li_or_span:
            possible_next_siblings = active_li_or_span.find_next_siblings(["li", "a"])
            for sibling in possible_next_siblings:
                if sibling.name == "a" and sibling.get("href"):
                    next_item_tag = sibling
                    break
                elif sibling.name == "li" and sibling.find("a", href=True):
                    next_item_tag = sibling.find("a", href=True)
                    break
        else:
            next_link_texts = ["Next", "Sau", "Trang kế", "Tiếp", "»", ">"]
            for text_pattern in next_link_texts:
                # Tìm kiếm kỹ hơn cho nút "Next"
                _next_item_candidate = pagination.find("a", string=re.compile(f"\\s*{re.escape(text_pattern)}\\s*", re.IGNORECASE))
                if not _next_item_candidate:
                     _next_item_candidate = pagination.find("a", title=re.compile(f"\\s*{re.escape(text_pattern)}\\s*", re.IGNORECASE))
                if not _next_item_candidate:
                     _next_item_candidate = pagination.find("a", attrs={"aria-label": re.compile(f"\\s*{re.escape(text_pattern)}\\s*", re.IGNORECASE)})

                if _next_item_candidate:
                    next_item_tag = _next_item_candidate
                    break
        
        if next_item_tag: # Sử dụng next_item_tag đã tìm được
            next_page_relative_url = next_item_tag.get("href")
            if next_page_relative_url and next_page_relative_url != "#" and not next_page_relative_url.startswith("javascript:"):
                parsed_current_url = urlparse(genre_page_url)
                if next_page_relative_url.startswith("?"):
                    next_page_url = urljoin(f"{parsed_current_url.scheme}://{parsed_current_url.netloc}{parsed_current_url.path}", next_page_relative_url)
                elif next_page_relative_url.startswith("/"):
                    next_page_url = urljoin(f"{parsed_current_url.scheme}://{parsed_current_url.netloc}", next_page_relative_url)
                elif not next_page_relative_url.startswith("http"):
                    base_path_for_pagination = re.sub(r"/(trang-\d+|page/\d+)/?$", "/", parsed_current_url.path)
                    if not base_path_for_pagination.endswith("/"): base_path_for_pagination += "/"
                    next_page_relative_url_cleaned = next_page_relative_url.lstrip("/")
                    next_page_url = urljoin(f"{parsed_current_url.scheme}://{parsed_current_url.netloc}{base_path_for_pagination}", next_page_relative_url_cleaned)
                else:
                    next_page_url = next_page_relative_url

                if next_page_url and urlparse(next_page_url)._replace(fragment="").geturl() == parsed_current_url._replace(fragment="").geturl():
                    logger.warning(f"URL trang tiếp theo ({next_page_url}) giống hệt trang hiện tại ({genre_page_url}). Dừng phân trang.")
                    next_page_url = None
    
    unique_stories_on_page = []
    seen_story_urls_on_page = set()
    for s_item in stories:
        if s_item["url"] not in seen_story_urls_on_page:
            unique_stories_on_page.append(s_item)
            seen_story_urls_on_page.add(s_item["url"])

    logger.info(f"Lấy được {len(unique_stories_on_page)} truyện từ trang: {genre_page_url}")
    if next_page_url:
        logger.debug(f"  URL trang truyện tiếp theo của thể loại: {next_page_url}")
    else:
        logger.debug(f"  Không tìm thấy trang truyện tiếp theo của thể loại từ {genre_page_url}")
    return unique_stories_on_page, next_page_url

# --- GET ALL STORIES FROM GENRE ---
# (Hàm này bạn đã cung cấp trong câu hỏi trước và đã có logger, type hint)
# Đảm bảo hàm này giữ nguyên hoặc được cập nhật nếu cần
def get_all_stories_from_genre(genre_name: str, genre_url: str,
                               max_pages_to_crawl: Optional[int] = MAX_STORIES_PER_GENRE_PAGE
                               ) -> List[Dict[str, Any]]:
    all_stories_in_genre: List[Dict[str, Any]] = []
    current_page_url: Optional[str] = genre_url
    page_count = 0
    visited_page_urls: set[str] = set()

    logger.info(f"Bắt đầu lấy tất cả truyện từ thể loại '{genre_name}': {genre_url}")

    while current_page_url and current_page_url not in visited_page_urls:
        if current_page_url in visited_page_urls:
            logger.warning(f"Đã truy cập trang {current_page_url} trước đó trong cùng một lần crawl thể loại. Dừng để tránh vòng lặp.")
            break
        visited_page_urls.add(current_page_url)

        page_count += 1
        if max_pages_to_crawl is not None and page_count > max_pages_to_crawl:
            logger.info(f"Đã đạt giới hạn {max_pages_to_crawl} trang cho thể loại {genre_name} ({genre_url}).")
            break

        logger.info(f"Đang xử lý trang {page_count} của thể loại '{genre_name}': {current_page_url}")
        stories_on_page, next_page_url_candidate = get_stories_from_genre_page(current_page_url) # Gọi hàm đã sửa ở trên

        if not stories_on_page and page_count > 1:
            logger.info(f"Không có truyện nào trên trang {current_page_url} (trang {page_count} của thể loại '{genre_name}').")

        new_stories_added_count = 0
        for story in stories_on_page:
            is_duplicate = any(s["url"] == story["url"] for s in all_stories_in_genre)
            if not is_duplicate:
                all_stories_in_genre.append(story)
                new_stories_added_count += 1
        if new_stories_added_count < len(stories_on_page) and stories_on_page:
            logger.debug(f"Đã loại bỏ {len(stories_on_page) - new_stories_added_count} truyện trùng lặp từ trang {current_page_url}.")

        if next_page_url_candidate and next_page_url_candidate != current_page_url:
            parsed_next = urlparse(next_page_url_candidate)
            parsed_current = urlparse(current_page_url)
            if parsed_next._replace(fragment="").geturl() == parsed_current._replace(fragment="").geturl():
                logger.warning(f"URL trang tiếp theo ({next_page_url_candidate}) sau khi chuẩn hóa giống trang hiện tại. Dừng phân trang thể loại '{genre_name}'.")
                current_page_url = None
            else:
                current_page_url = next_page_url_candidate
        else:
            if next_page_url_candidate == current_page_url and next_page_url_candidate is not None:
                logger.info(f"URL trang tiếp theo giống trang hiện tại ({current_page_url}). Dừng phân trang thể loại '{genre_name}'.")
            elif not next_page_url_candidate:
                logger.info(f"Không tìm thấy trang tiếp theo cho thể loại '{genre_name}' từ trang {current_page_url}, kết thúc crawl thể loại này.")
            current_page_url = None
    
    unique_stories_final = []
    seen_story_urls_final = set()
    for story_item in all_stories_in_genre:
        if story_item["url"] not in seen_story_urls_final:
            unique_stories_final.append(story_item)
            seen_story_urls_final.add(story_item["url"])

    logger.info(
        f"Tổng cộng lấy được {len(unique_stories_final)} truyện từ thể loại '{genre_name}' ({genre_url}) sau khi xử lý {page_count} trang."
    )
    return unique_stories_final


# --- GET CHAPTER NUMBER (HELPER) ---
def get_chapter_number(chapter_item: Dict[str, str]) -> float: # Sửa: Thêm type hint
    title = chapter_item.get("title", "")
    url = chapter_item.get("url", "")

    match_title = re.search(
        r"(?:Chương|Ch\.|Quyển\s*\d+\s*-\s*Chương|Chapter|Chương\s*Thứ)\s*(\d+)",
        title, re.IGNORECASE
    )
    if match_title: return float(match_title.group(1)) # Sửa: int -> float

    match_title_plain_num = re.match(r"(\d+)\s*[:.-]", title)
    if match_title_plain_num: return float(match_title_plain_num.group(1)) # Sửa: int -> float

    match_url = re.search(r"(?:chuong|chap|chapter|quyen)[-/](\d+)", url, re.IGNORECASE)
    if match_url: return float(match_url.group(1)) # Sửa: int -> float

    if title.isdigit(): return float(title) # Sửa: int -> float
    
    all_numbers_in_title = re.findall(r'\d+', title)
    if all_numbers_in_title:
        try:
            return float(all_numbers_in_title[-1])
        except ValueError:
            pass
    # logger.debug(f"Không thể trích xuất số chương từ: Tiêu đề='{title}', URL='{url}'. Sẽ xếp cuối.") # Bỏ log này vì quá nhiều
    return float("inf")


# --- GET CHAPTERS FROM STORY ---
def get_chapters_from_story(story_url: str,
                            story_title: str, # <<< THAM SỐ QUAN TRỌNG ĐÃ THÊM
                            max_pages: int = MAX_CHAPTER_PAGES_TO_CRAWL # Sửa: Đổi tên tham số cho nhất quán
                           ) -> List[Dict[str, str]]:
    logger.info(f"Truyện '{story_title}': Đang lấy danh sách chương từ URL: {story_url}") # Sửa: print -> logger.info và dùng story_title
    all_chapters: List[Dict[str, str]] = [] # Sửa: Type hint

    initial_response = make_request(story_url)
    if not initial_response or not initial_response.text: # Sửa: Kiểm tra thêm response.text
        logger.error(f"Truyện '{story_title}': Không nhận được phản hồi hoặc nội dung rỗng từ trang truyện chính {story_url}")
        return []
    initial_soup = BeautifulSoup(initial_response.text, "html.parser")

    current_chapter_list_page_url: Optional[str] = story_url # Sửa: Type hint

    list_chapter_selectors = [
        "a.list-chapter", "a.btn-show-all-chapters", "a.btn-list-chapter",
        'a[href*="danh-sach-chuong"]', 'a[title*="Danh sách chương"]',
    ]
    list_chapter_link_tag = None
    for selector in list_chapter_selectors:
        candidate = initial_soup.select_one(selector)
        if candidate and candidate.get("href"):
            list_chapter_link_tag = candidate
            break
    if not list_chapter_link_tag:
        list_chapter_link_tag = initial_soup.find(
            "a", string=re.compile(r"Xem\s+tất\s+cả\s+chương|Danh\s+sách\s+chương|All\s+Chapters", re.IGNORECASE)
        )

    if list_chapter_link_tag and list_chapter_link_tag.get("href") and list_chapter_link_tag.get("href") != "#":
        candidate_url = urljoin(story_url, list_chapter_link_tag["href"])
        if urlparse(candidate_url).path != urlparse(story_url).path:
            current_chapter_list_page_url = candidate_url
            logger.info(f"Truyện '{story_title}': Tìm thấy link đến trang danh sách chương riêng: {current_chapter_list_page_url}") # Sửa: print -> logger.info
        else:
            logger.debug(f"Truyện '{story_title}': Link 'Danh sách chương' ({candidate_url}) có vẻ vẫn là trang truyện.") # Sửa: print -> logger.debug
    else:
        logger.debug(f"Truyện '{story_title}': Không tìm thấy link 'Danh sách chương' riêng biệt, sẽ lấy chương từ trang truyện chính.") # Sửa: print -> logger.debug

    page_num = 1
    processed_chapter_urls: set[str] = set() # Sửa: Type hint
    visited_chapter_list_pages: set[str] = set() # Sửa: Type hint

    while (current_chapter_list_page_url and
           current_chapter_list_page_url not in visited_chapter_list_pages and
           page_num <= max_pages): # Sửa: dùng max_pages

        if current_chapter_list_page_url in visited_chapter_list_pages:
            logger.warning(f"Truyện '{story_title}': Trang danh sách chương {current_chapter_list_page_url} đã được xử lý. Dừng.") # Sửa: print -> logger.warning
            break
        visited_chapter_list_pages.add(current_chapter_list_page_url)

        logger.info(f"Truyện '{story_title}': Đang lấy chương từ trang DS chương: {current_chapter_list_page_url} (Trang DS thứ {page_num})") # Sửa: print -> logger.info
        response_chapters = make_request(current_chapter_list_page_url)
        if not response_chapters or not response_chapters.text: # Sửa: Kiểm tra thêm text
            logger.warning(f"Truyện '{story_title}': Không có phản hồi/nội dung từ {current_chapter_list_page_url}")
            break
        soup_chapters = BeautifulSoup(response_chapters.text, "html.parser")

        chapter_list_container = soup_chapters.find("div", id="list-chapter")
        if not chapter_list_container:
            chapter_list_container = soup_chapters.find(["ul", "div"], class_=re.compile(r"list-chapter|ds-chuong|chapter-list|version-chap"))
        
        if not chapter_list_container:
            if page_num == 1 and urlparse(current_chapter_list_page_url).path == urlparse(story_url).path:
                logger.debug(f"Truyện '{story_title}': Không tìm thấy container chương cụ thể trên trang truyện chính, thử tìm link chương trên toàn trang.") # Sửa: print -> logger.debug
                chapter_list_container = soup_chapters # Tìm trên toàn trang
            else:
                logger.warning(f"Truyện '{story_title}': Không tìm thấy container chứa danh sách chương trên: {current_chapter_list_page_url}") # Sửa: print -> logger.warning
                break

        chapter_links = chapter_list_container.find_all("a", href=re.compile(r"(chuong|chap|chapter|quyen)[-/]\d+", re.IGNORECASE))
        if not chapter_links and page_num == 1:
            logger.warning(f"Truyện '{story_title}': Không tìm thấy link chương nào khớp mẫu trên trang {current_chapter_list_page_url}.") # Sửa: print -> logger.warning

        found_new_chapters_on_this_page = False
        temp_chapters_on_page: List[Dict[str, str]] = [] # Sửa: Type hint
        for link in chapter_links:
            chapter_href = link.get("href")
            chapter_title_text = link.get_text(strip=True) or link.get("title", "").strip() or f"Chương không tên {len(all_chapters) + len(temp_chapters_on_page) + 1}" # Sửa: Thêm fallback

            if chapter_href and chapter_title_text:
                chapter_full_url = urljoin(current_chapter_list_page_url, chapter_href)
                if chapter_full_url not in processed_chapter_urls:
                    temp_chapters_on_page.append({"title": chapter_title_text, "url": chapter_full_url})
                    processed_chapter_urls.add(chapter_full_url)
                    found_new_chapters_on_this_page = True
                    logger.debug(f"  Truyện '{story_title}': Tìm thấy link chương: '{chapter_title_text}' - {chapter_full_url}")
        
        all_chapters.extend(temp_chapters_on_page)
        if not found_new_chapters_on_this_page and chapter_links and page_num > 1 :
            logger.debug(f"Truyện '{story_title}': Không tìm thấy chương mới trên trang {current_chapter_list_page_url} (các chương đã được xử lý trước đó).") # Sửa: print -> logger.debug
        elif not chapter_links and page_num > 1: # Sửa: elif thay vì if riêng
            logger.debug(f"Truyện '{story_title}': Không tìm thấy link chương nào trên trang phân trang DS chương: {current_chapter_list_page_url}") # Sửa: print -> logger.debug

        # Pagination for chapter list
        pagination_chapters = soup_chapters.find("ul", class_="pagination")
        if not pagination_chapters:
            pagination_chapters = soup_chapters.find("div", class_=re.compile(r"pagination|wp-pagenavi|page-nav"))

        next_page_chapter_url_candidate: Optional[str] = None # Sửa: Type hint
        if pagination_chapters:
            active_li_or_span = pagination_chapters.find(["li", "span"], class_=re.compile(r"active|current"))
            next_item_tag_chap = None # Sửa: Đổi tên biến
            if active_li_or_span:
                possible_next_siblings = active_li_or_span.find_next_siblings(["li", "a"])
                for sibling in possible_next_siblings:
                    if sibling.name == "a" and sibling.get("href"): next_item_tag_chap = sibling; break
                    elif sibling.name == "li" and sibling.find("a", href=True): next_item_tag_chap = sibling.find("a", href=True); break
            else:
                next_link_texts = ["Next", "Sau", "Trang kế", "Tiếp", "»", ">"]
                for text in next_link_texts:
                    candidate = pagination_chapters.find("a", string=re.compile(f"\\s*{re.escape(text)}\\s*", re.IGNORECASE))
                    if candidate: next_item_tag_chap = candidate; break
            
            if next_item_tag_chap:
                next_page_relative = next_item_tag_chap.get("href")
                if next_page_relative and next_page_relative != "#" and not next_page_relative.startswith("javascript:"):
                    next_page_chapter_url_candidate = urljoin(current_chapter_list_page_url, next_page_relative)
                    if urlparse(next_page_chapter_url_candidate)._replace(fragment="").geturl() == urlparse(current_chapter_list_page_url)._replace(fragment="").geturl():
                        next_page_chapter_url_candidate = None

        if next_page_chapter_url_candidate:
            current_chapter_list_page_url = next_page_chapter_url_candidate
            page_num += 1
        else:
            if page_num == 1 and not all_chapters:
                logger.warning(f"Truyện '{story_title}': Không tìm thấy chương nào trên trang truyện chính ({story_url}) và không có phân trang chương.") # Sửa: print -> logger.warning
            else:
                logger.info(f"Truyện '{story_title}': Không tìm thấy trang danh sách chương tiếp theo từ {current_chapter_list_page_url or story_url}.") # Sửa: print -> logger.info
            break

    all_chapters.sort(key=get_chapter_number) # Hàm get_chapter_number đã sửa thành trả về float

    final_unique_chapters: List[Dict[str, str]] = [] # Sửa: Type hint
    seen_final_chapter_urls: set[str] = set() # Sửa: Type hint
    for ch in all_chapters:
        if ch["url"] not in seen_final_chapter_urls:
            final_unique_chapters.append(ch)
            seen_final_chapter_urls.add(ch["url"])

    logger.info(f"Truyện '{story_title}': Tìm thấy tổng cộng {len(final_unique_chapters)} chương (sau khi lọc trùng và sắp xếp).") # Sửa: print -> logger.info
    return final_unique_chapters


# --- GET STORY CHAPTER CONTENT ---
def get_story_chapter_content(chapter_url: str,
                              chapter_title: str, # <<< THAM SỐ QUAN TRỌNG ĐÃ THÊM
                             ) -> Optional[str]:
    logger.info(f"Đang tải nội dung chương '{chapter_title}': {chapter_url}") # Sửa: print -> logger.info và dùng chapter_title
    response = make_request(chapter_url)
    if not response or not response.text: # Sửa: Kiểm tra thêm response.text
        logger.error(f"Chương '{chapter_title}': Không nhận được phản hồi hoặc nội dung rỗng từ {chapter_url}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    chapter_content_div = soup.find("div", id="chapter-c")
    if not chapter_content_div:
        chapter_content_div = soup.find("div", class_=re.compile(r"chapter-content|reading-content|entry-content|content-chapter|main-text"))
    if not chapter_content_div:
        chapter_content_div = soup.find("article", class_=re.compile(r"post-content|content"))

    if chapter_content_div:
        for br_tag in chapter_content_div.find_all("br"):
            br_tag.replace_with("\n")
        
        # Loại bỏ comment HTML bên trong nội dung chương
        for comment_html in chapter_content_div.find_all(string=lambda text_node: isinstance(text_node, Comment)): # Sửa: Đổi tên biến
            comment_html.extract()

        selectors_to_remove = [
            "div.ads-responsive", "div.incontent-ad", "div.ads-chapter",
            'div.text-center[style*="text-align: center"]', "div#ads-chapter-pc-top",
            "div#ads-chapter-google-bottom", "div#chapter-end-bot", 'div[id*="ads-google"]',
            'div[class*="ads"]', 'div[class*="advert"]', "div.box-notice", "hr.chapter-end",
            "hr.chapter-start", "div.chapter-nav", "div.nav-links", "div.navigation",
            "div.fb-comments", "div#comments", "div.comment-form", "div.author-info",
            "div.meta-data", 'div[style*="display:none"]', "button", "input", "select",
            "textarea", "iframe", ".code-block", ".wp-block-code", ".sharedaddy", ".share-buttons",
            # Thêm các selector cụ thể hơn nếu cần
            "div.truyen-title", # Tiêu đề truyện có thể lặp lại trong nội dung chương
            "h1.chapter-title", "h2.chapter-title", "h3.chapter-title", # Tiêu đề chương
        ]
        for selector in selectors_to_remove:
            try:
                for unwanted_element in chapter_content_div.select(selector):
                    if unwanted_element != chapter_content_div:
                        unwanted_element.decompose()
            except Exception as e_select:
                logger.debug(f"  Lỗi khi thử loại bỏ bằng selector '{selector}' cho chương '{chapter_title}': {e_select}") # Sửa: print -> logger.debug

        for s_tag in chapter_content_div.find_all(["script", "style", "noscript", "link"]):
            s_tag.decompose()

        for empty_tag in chapter_content_div.find_all(["div", "p", "span"]):
            if not empty_tag.get_text(strip=True) and not empty_tag.find_all(True, recursive=False):
                if not empty_tag.find("img"):
                    empty_tag.decompose()

        chapter_text = chapter_content_div.get_text(separator="\n", strip=False)
        lines = chapter_text.split("\n")
        cleaned_lines = []
        consecutive_empty_lines = 0
        patterns_to_remove_strict = [
            r"^(Nguồn|Edit|Người Edit|Biên tập|Dịch|Chuyển ngữ|BTV|Tác giả|Tên truyện|Chương)\s*[:：].*$", # Mở rộng thêm
            r"Bạn đang đọc truyện tại .*?(truyenfull|sstruyen|webtruyen|nettruyen|tangthuvien|wattpad|goctruyen|metruyenchu|bachngocsach|dtruyen|vlognovel|santruyen|thichdoctruyen|truyenyy| vipfic)\.(com|vn|net|org|info|cc)", # Thêm domain
            r"đọc truyện online miễn phí", r"truyện chữ online", r"^\s*Mời bạn đọc thêm truyện.*?$",
            r"\((?:Còn tiếp|Còn nữa|Hết chương|Kết thúc chương \d+)\)",
            r"---+\s*oOo\s*---+", r"=========", r"^\*\s*\*\s*\*\s*$",
            r"Tìm kiếm với từ khóa:", r"đọc truyện hay", r"Website:",
            # Loại bỏ tên chương/truyện nếu nó xuất hiện đơn lẻ trên 1 dòng
            r"^\s*(Chương|Quyển)\s*\d+\s*[:：.]?\s*.*$", # Bao quát hơn
            r"^\s*Hết\s*chương\s*\d*\s*$",
            r"Mời các bạn xem và đọc tại .*?\.", r"Truy cập .*? để đọc nhiều truyện hay",
            r"Thảo luận truyện tại .*?", r"Like fanpage .*? để cập nhật nhanh nhất",
            r"Scan by", r"Converted by", r"Ps by", r"Pr by",
            r"Mọi người bình chọn tốt cho mình nhé", r"Chúc các bạn đọc truyện vui vẻ",
            r"Nếu bạn thấy hay, hãy chia sẻ cho mọi người", r"Xin cảm ơn!", r"Thank you!",
            r"load(?:\s*chapter)?\s*error", r"Server \d+ - SL \d+", r"Đọc Full Tại",
            r"Tổng hợp bởi", r"\(づ｡◕‿‿◕｡\)づ", # Giảm bớt để tránh false positive
            r"Nếu thấy hay vui lòng nhấn like và theo dõi", r"Xem thêm nhiều truyện hot tại",
            r" Truyện được cung cấp bởi .*?", # Thêm
            r"Đọc truyện online hay nhất tại .*?" # Thêm
        ]

        for line in lines:
            stripped_line = line.strip()
            is_unwanted = False
            if len(stripped_line) < 150: # Tăng ngưỡng một chút
                for pattern in patterns_to_remove_strict:
                    if re.search(pattern, stripped_line, re.IGNORECASE):
                        is_unwanted = True
                        logger.debug(f"  Chương '{chapter_title}': Loại bỏ dòng (strict) khớp pattern '{pattern}': '{stripped_line[:70]}...'")
                        break
            
            if not stripped_line:
                consecutive_empty_lines +=1
                if consecutive_empty_lines <= 2 :
                    cleaned_lines.append("")
                continue
            else:
                consecutive_empty_lines = 0
            
            if not is_unwanted:
                cleaned_lines.append(stripped_line)

        final_text = "\n".join(cleaned_lines).strip()
        final_text = re.sub(r"([._\-\*\=\#\~\s])\1{5,}", r"\1\1\1", final_text)
        final_text = re.sub(r"\n\s*\n\s*\n+", "\n\n", final_text)

        if len(final_text.split()) < 20 and len(final_text) > 5: # Ngưỡng tối thiểu ký tự
            logger.warning(
                f"  Chương '{chapter_title}': Nội dung ({chapter_url}) rất ngắn ({len(final_text.split())} từ) sau khi làm sạch."
            )
            # logger.debug(f"  Nội dung gốc (đã lọc selector) cho '{chapter_title}': {chapter_content_div.get_text(separator='\\n', strip=True)[:300]}...")
            # logger.debug(f"  Nội dung sau lọc cho '{chapter_title}': {final_text[:300]}...")
        elif not final_text and response.text: # Rỗng sau khi lọc nhưng response có data
             logger.warning(f"  Chương '{chapter_title}': Nội dung ({chapter_url}) trống sau khi làm sạch. HTML gốc có thể không chứa text truyện hoặc bị lọc hết.")


        return final_text if final_text else None # Sửa: Trả về None nếu final_text rỗng
    else:
        logger.error(f"Chương '{chapter_title}': Không tìm thấy div nội dung chương (ví dụ: #chapter-c) tại URL: {chapter_url}") # Sửa: print -> logger.error
        body_content = soup.body
        if body_content:
            for s_tag in body_content.find_all(["script", "style", "noscript", "header", "footer", "nav", "aside"]):
                s_tag.decompose()
            for comment_html_body in body_content.find_all(string=lambda text_node: isinstance(text_node, Comment)): # Sửa: Đổi tên biến
                comment_html_body.extract()

            body_text = body_content.get_text(separator="\n", strip=True)
            body_text = re.sub(r"\n\s*\n+", "\n\n", body_text)
            if len(body_text.split()) > 50:
                logger.info(f"  Chương '{chapter_title}': Lấy nội dung từ body do không tìm thấy container chương cụ thể. Độ dài: {len(body_text.split())} từ.") # Sửa: print -> logger.info
                return body_text
            else:
                logger.debug(f"  Chương '{chapter_title}': Nội dung từ body quá ngắn hoặc không phù hợp.") # Sửa: print -> logger.debug
        return None