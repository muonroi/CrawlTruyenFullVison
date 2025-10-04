import httpx
html = httpx.get('https://tangthuvien.net', timeout=20).text
for token in ['menuData', 'categoryData', 'categories', 'listCategory', 'navCategory']:
    idx = html.lower().find(token.lower())
    print(token, idx)
    if idx != -1:
        print(html[idx:idx+400].encode('utf-8'))
