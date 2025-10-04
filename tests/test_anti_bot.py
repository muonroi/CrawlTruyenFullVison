from utils.anti_bot import is_anti_bot_content


def test_is_anti_bot_content_detects():
    assert is_anti_bot_content("Bạn đã bị chặn truy cập tự động")


def test_is_anti_bot_content_normal():
    assert not is_anti_bot_content("Nội dung chương bình thường")


def test_is_anti_bot_content_with_html_signature():
    html = """
    <html>
        <head><title>Just a moment...</title></head>
        <body>
            <h1>Checking your browser before accessing</h1>
            <p>Please enable JavaScript and cookies to continue.</p>
            <script>var cf_browser_verification = true;</script>
        </body>
    </html>
    """
    assert is_anti_bot_content(html)


def test_is_anti_bot_content_with_normal_html():
    html = """
    <html>
        <head><title>Chương truyện mới</title></head>
        <body>
            <h1>Chương 1: Khởi đầu</h1>
            <p>Nội dung chương truyện bình thường.</p>
        </body>
    </html>
    """
    assert not is_anti_bot_content(html)
