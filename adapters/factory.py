def get_adapter(site_key: str):
    if site_key == "xtruyen":
        from adapters.xtruyen_adapter import XTruyenAdapter
        return XTruyenAdapter()
    if site_key == "tangthuvien":
        from adapters.tangthuvien_adapter import TangThuVienAdapter
        return TangThuVienAdapter()
    raise ValueError(f"Unknown site: {site_key}")

