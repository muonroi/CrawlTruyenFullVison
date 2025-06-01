from bs4 import BeautifulSoup, Comment

from utils.logger import logger
from utils.io_utils import filter_lines_by_patterns

# Step 1: Clean HTML tags
def clean_chapter_content(chapter_div):
    for tag in chapter_div.find_all(['script', 'style']):
        tag.decompose()
    for tag in chapter_div.find_all(class_=lambda x: x and ('ads' in x or 'notice' in x or 'box-notice' in x)):
        tag.decompose()
    for element in chapter_div(text=lambda text: isinstance(text, Comment)):
        element.extract()
    return chapter_div
