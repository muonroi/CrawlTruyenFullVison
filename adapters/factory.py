from .truyenfull_adapter import TruyenFullAdapter

def get_adapter(site_key: str):
    if site_key == "truyenfull":
        return TruyenFullAdapter()
    elif site_key == "metruyenfull":
        from adapters.metruyenfull_adapter import MeTruyenFullAdapter
        return MeTruyenFullAdapter()
    # elif site_key == "vivutruyen":
    #     from adapters.vivutruyen_adapter import VivuTruyenAdapter
    #     return VivuTruyenAdapter()
    raise ValueError(f"Unknown site: {site_key}")
