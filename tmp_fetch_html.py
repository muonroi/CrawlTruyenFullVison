import asyncio
from adapters.tangthuvien_adapter import TangThuVienAdapter

async def main():
    adapter = TangThuVienAdapter()
    html = await adapter._fetch_text(adapter.base_url, wait_for_selector="div.update-wrap")
    if not html:
        print('no html')
        return
    lower_html = html.lower()
    markers = ["/the-loai/", "ctg=", "category"]
    for marker in markers:
        idx = lower_html.find(marker)
        print(f"marker={marker} index={idx}")
        if idx != -1:
            snippet = html[max(0, idx-80):idx+200]
            print(snippet.encode('utf-8'))

asyncio.run(main())
