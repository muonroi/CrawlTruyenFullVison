def test_is_anti_bot_content_detects():
    from utils.anti_bot import is_anti_bot_content
    assert is_anti_bot_content("Bạn đã bị chặn truy cập tự động")

def test_is_anti_bot_content_normal():
    from utils.anti_bot import is_anti_bot_content
    assert not is_anti_bot_content("Nội dung chương bình thường")
