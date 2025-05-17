from .truyenfull_adapter import TruyenFullAdapter

def get_adapter(site_key: str):
    if site_key == "truyenfull":
        return TruyenFullAdapter()
    elif site_key == "metruyenfull":
        from adapters.metruyenfull_adapter import MeTruyenFullAdapter
        return MeTruyenFullAdapter()
    raise ValueError(f"Unknown site: {site_key}")
