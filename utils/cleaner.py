from bs4 import Comment


# Step 1: Clean HTML tags
def clean_chapter_content(chapter_div):
    for tag in chapter_div.find_all(['script', 'style']):
        tag.decompose()
    for tag in chapter_div.find_all(class_=lambda x: x and ('ads' in x or 'notice' in x or 'box-notice' in x)):
        tag.decompose()
    for element in chapter_div(text=lambda text: isinstance(text, Comment)):
        element.extract()
    return chapter_div

def ensure_sources_priority(sources: list) -> list:
    """Thêm priority mặc định vào các nguồn (nếu chưa có)."""
    default_priority = {
        "truyenyy": 1,
        "truyenfull": 2,
        "metruyenfull": 3,
    }
    for source in sources:
        if "priority" not in source or source["priority"] is None:
            sk = source.get("site_key", "").lower()
            source["priority"] = default_priority.get(sk, 100)
    return sources
