def get_adapter(site_key: str):
    if site_key == "xtruyen":
        from adapters.xtruyen_adapter import XTruyenAdapter
        return XTruyenAdapter()
    raise ValueError(f"Unknown site: {site_key}")