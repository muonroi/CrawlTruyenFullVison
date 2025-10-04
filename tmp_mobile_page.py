import httpx
html = httpx.get('https://m.tangthuvien.net/danh-muc', timeout=20).text
lower = html.lower()
print('ctg index', lower.find('ctg='))
print('the-loai index', lower.find('/the-loai/'))
start = lower.find('list-category')
print('list-category', start)
if start != -1:
    print(html[start:start+400].encode('utf-8'))
