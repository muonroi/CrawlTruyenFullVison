from enum import Enum

class CrawlError(str, Enum):
    TIMEOUT = "timeout"
    ANTI_BOT = "anti_bot"
    DEAD_LINK = "dead_link"
    WRITE_FAIL = "write_fail"
    UNKNOWN = "unknown"
