from .truyenfull_adapter import TruyenFullAdapter

def get_adapter(site_key: str):
    if site_key == "truyenfull":
        return TruyenFullAdapter()
    elif site_key == "metruyenfull":
        from adapters.metruyenfull_adapter import MeTruyenFullAdapter
        return MeTruyenFullAdapter()
    elif site_key == "truyenyy":
        from adapters.truyenyy_adapter import TruyenYYAdapter
        return TruyenYYAdapter()
    elif site_key == "xtruyen":
        from adapters.xtruyen_adapter import XTruyenAdapter
        return XTruyenAdapter()
    raise ValueError(f"Unknown site: {site_key}")
