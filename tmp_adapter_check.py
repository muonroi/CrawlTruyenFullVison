import asyncio
from adapters.tangthuvien_adapter import TangThuVienAdapter

async def main():
    adapter = TangThuVienAdapter()
    genres = await adapter.get_genres()
    print('genres', len(genres))
    for g in genres:
        print(g['name'], g['url'])

asyncio.run(main())
