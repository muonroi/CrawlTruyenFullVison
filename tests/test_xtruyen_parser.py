
import os
import pytest
from analyze.xtruyen_parse import parse_story_info

# Xác định đường dẫn tuyệt đối đến file test data
# Điều này giúp test chạy được từ bất kỳ đâu
TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'site_info', 'xtruyen')
DETAIL_STORY_FILE = os.path.join(TEST_DATA_DIR, 'detail_story.txt')

@pytest.mark.skipif(not os.path.exists(DETAIL_STORY_FILE), reason="Test data file not found")
def test_parse_story_info_from_file():
    """
    Test a story detail page using a local HTML file to ensure correct parsing,
    especially for the 'categories' structure.
    """
    with open(DETAIL_STORY_FILE, 'r', encoding='utf-8') as f:
        html_content = f.read()

    # Gọi hàm parse với nội dung HTML từ file
    data = parse_story_info(html_content, base_url="https://xtruyen.vn")

    # 1. Kiểm tra các thông tin cơ bản
    assert data['title'] == 'THỜI BUỔI NÀY AI CÒN ĐƯƠNG ĐỨNG ĐẮN HỒ YÊU'
    assert data['author'] == 'Ngã Lai Tạng Ba Binh Tuyến'
    assert 'Ta kêu Trần Nguyên' in data['description']
    assert data['status'] == 'Hoàn thành'

    # 2. Kiểm tra cấu trúc của 'categories' - đây là phần quan trọng nhất
    assert 'categories' in data
    categories = data['categories']
    
    # Phải là một list
    assert isinstance(categories, list)
    # Phải có ít nhất 1 phần tử
    assert len(categories) > 0
    
    # Phần tử đầu tiên phải là một dictionary
    first_category = categories[0]
    assert isinstance(first_category, dict)
    
    # Dictionary đó phải chứa 'name' và 'url'
    assert 'name' in first_category
    assert 'url' in first_category
    
    # Kiểm tra giá trị cụ thể
    assert first_category['name'] == 'Dị Giới'
    assert first_category['url'] == 'https://xtruyen.vn/theloai/di-gioi/'

    # 3. Kiểm tra danh sách chapter
    assert 'chapters' in data
    assert isinstance(data['chapters'], list)
    # Dựa vào file HTML, trang này không inline chapter list, nó được load sau
    # nên ở bước này list rỗng là đúng
    assert len(data['chapters']) == 0
