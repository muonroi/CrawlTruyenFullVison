from .truyenfull_adapter import TruyenFullAdapter

def get_adapter(site_key: str):
    if site_key == "truyenfull":
        return TruyenFullAdapter()
    raise ValueError(f"Unknown site: {site_key}")
