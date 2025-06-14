import ast
from pathlib import Path
from types import SimpleNamespace
import re
import pytest
from bs4 import BeautifulSoup
from urllib.parse import urljoin


def load_funcs(path, names, extra=None):
    source = Path(path).read_text()
    module = ast.parse(source)
    ns = {'BeautifulSoup': BeautifulSoup, 'urljoin': urljoin, 're': re}
    if extra:
        ns.update(extra)
    for node in module.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names:
            exec(ast.get_source_segment(source, node), ns)
    return [ns[n] for n in names]


build_category_list_url, parse_chapters_from_soup = load_funcs(
    'analyze/truyenyy_parse.py', ['build_category_list_url', 'parse_chapters_from_soup'])

absolutize, = load_funcs('analyze/vivutruyen_parse.py', ['absolutize'])

get_input_value, = load_funcs('analyze/truyenfull_vision_parse.py', ['get_input_value'])

# For get_story_metadata we stub make_request and logger
async def fake_request(url, site):
    return SimpleNamespace(text=FAKE_HTML)

class DummyLogger:
    def info(self, *a, **kw):
        pass
    def warning(self, *a, **kw):
        pass

get_story_metadata, = load_funcs(
    'analyze/metruyenfull_parse.py', ['get_story_metadata'],
    extra={'make_request': fake_request, 'logger': DummyLogger(), 'get_chapters_from_story': lambda *a, **k: []})

FAKE_HTML = """
<h1 class='title'>Title</h1>
<div class='info-holder'>
    <a itemprop='author' title='Au'></a>
    <a itemprop='genre' href='/g'>G</a>
    <span class='label-success'>200 chương</span>
    <img itemprop='image' src='c.jpg'>
</div>
<div class='desc-text desc-text-full'>Desc</div>
"""


def test_truyenyy_build_url():
    base = 'https://yy/truyen'
    assert build_category_list_url(base, 1) == base
    assert build_category_list_url(base, 3) == f'{base}?p=3'


def test_truyenyy_parse_chapters():
    html = """
    <ul class='flex flex-col w-full divide-y'>
        <li><a class='flex flex-row items-center' href='/c1'><p class='flex-1 font-[300] line-clamp-2'>T1</p></a></li>
    </ul>"""
    soup = BeautifulSoup(html, 'html.parser')
    chs = parse_chapters_from_soup(soup, 'https://yy')
    assert chs == [{'url': 'https://yy/c1', 'title': 'T1'}]


def test_vivutruyen_absolutize():
    assert absolutize('/a') == 'https://vivutruyen.com/a'
    assert absolutize('http://b') == 'http://b'


def test_truyenfull_get_input_value():
    soup = BeautifulSoup('<input id="x" value="1">', 'html.parser')
    assert get_input_value(soup, 'x') == '1'
    assert get_input_value(soup, 'y', default='d') == 'd'


@pytest.mark.asyncio
async def test_metruyenfull_get_story_metadata():
    adapter = SimpleNamespace(SITE_KEY='metruyenfull')
    data = await get_story_metadata(adapter, 'u')
    assert data['title'] == 'Title'
    assert data['author'] == 'Au'
    assert data['description'] == 'Desc'
    assert data['categories'] == [{'name': 'G', 'url': '/g'}]
    assert data['total_chapters_on_site'] == 200
    assert data['cover'] == 'c.jpg'
