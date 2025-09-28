import re
from functools import lru_cache
from bs4 import BeautifulSoup
from utils.io_utils import load_patterns
from config.config import ANTI_BOT_PATTERN_FILE

@lru_cache()
def get_anti_bot_patterns():
    """Tải các pattern anti-bot từ file."""
    return load_patterns(ANTI_BOT_PATTERN_FILE)

def is_anti_bot_content(text: str) -> bool:
    """
    Kiểm tra xem nội dung HTML có phải là trang anti-bot hay không bằng nhiều phương pháp.
    """
    if not text:
        return False

    lower_text = text.lower()
    # Refined signature phrases to be more specific and avoid false positives.
    signature_phrases = [
        "bạn đã bị chặn",
        "checking your browser",
        "ddos protection by cloudflare",  # More specific
        "verifying you are human",
        "enable javascript and cookies to continue",
        "cloudflare's security check"
    ]
    if any(sig in lower_text for sig in signature_phrases):
        return True

    # The simple "cloudflare" check is too broad. A more nuanced check is needed.
    if "just a moment" in lower_text and "cloudflare" in lower_text:
        return True

    if len(text) < 100:
        return False  # Bỏ qua nếu nội dung quá ngắn hoặc rỗng

    # ===== Phương pháp 2: Phân tích cấu trúc HTML (tốn tài nguyên hơn) =====
    try:
        soup = BeautifulSoup(text, 'html.parser')

        # 2a. Kiểm tra tiêu đề trang
        title_tag = soup.find('title')
        if title_tag:
            lower_title = title_tag.get_text().lower()
            challenge_phrases = [
                "just a moment", "checking your browser", "ddos",
                "cloudflare", "attention required", "access denied", "checking connection"
            ]
            if any(phrase in lower_title for phrase in challenge_phrases):
                return True

        # 2b. Kiểm tra body "nghèo nàn" nhưng chứa script challenge
        body_tag = soup.body
        if body_tag:
            body_text = body_tag.get_text(strip=True)
            # Giả định: Nếu body có ít hơn 50 từ và trong HTML có chữ "javascript"
            # khả năng cao đây là trang thử thách JS.
            if len(body_text.split()) < 50 and "javascript" in lower_text:
                # Thêm một bước kiểm tra để chắc chắn hơn
                if "enable javascript" in lower_text or "turn on javascript" in lower_text:
                    return True

    except Exception:
        # Nếu parse HTML thất bại, có thể đó là một trang không hợp lệ, không phải anti-bot
        return False

    return False
