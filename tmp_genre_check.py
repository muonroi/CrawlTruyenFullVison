import httpx
from analyze.tangthuvien_parse import parse_genres

html = httpx.get('https://tangthuvien.net', timeout=20).text
genres = parse_genres(html, 'https://tangthuvien.net')
print('genres', len(genres))
for item in genres[:10]:
    print(item)
