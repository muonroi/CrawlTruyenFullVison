import sys, os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pytest
from bs4 import BeautifulSoup

from analyze import (
    truyenyy_parse,
    vivutruyen_parse,
    truyenfull_vision_parse,
    metruyenfull_parse,
)


def test_truyenyy_build_url():
    base = "https://yy/truyen"
    assert truyenyy_parse.build_category_list_url(base, 1) == base
    assert truyenyy_parse.build_category_list_url(base, 3) == f"{base}?p=3"


def test_truyenyy_parse_chapters():
    html = """
    <ul class='flex flex-col w-full divide-y'>
        <li><a class='flex flex-row items-center' href='/c1'><p class='flex-1 font-[300] line-clamp-2'>T1</p></a></li>
    </ul>"""
    soup = BeautifulSoup(html, "html.parser")
    chs = truyenyy_parse.parse_chapters_from_soup(soup, "https://yy")
    assert chs == [{"url": "https://yy/c1", "title": "T1"}]


def test_vivutruyen_absolutize():
    assert vivutruyen_parse.absolutize("/a") == "https://vivutruyen.com/a"
    assert vivutruyen_parse.absolutize("http://b") == "http://b"


def test_truyenfull_get_input_value():
    soup = BeautifulSoup('<input id="x" value="1">', "html.parser")
    assert truyenfull_vision_parse.get_input_value(soup, "x") == "1"
    assert truyenfull_vision_parse.get_input_value(soup, "y", default="d") == "d"


@pytest.mark.asyncio
async def test_metruyenfull_get_story_metadata(monkeypatch):
    html = """
    <h1 class='title'>Title</h1>
    <div class='info-holder'>
        <a itemprop='author' title='Au'></a>
        <a itemprop='genre' href='/g'>G</a>
        <span class='label-success'>200 chương</span>
        <img itemprop='image' src='c.jpg'>
    </div>
    <div class='desc-text desc-text-full'>Desc</div>
    """

    class Resp:
        def __init__(self, text):
            self.text = text

    async def fake_req(url, site):
        return Resp(html)

    monkeypatch.setattr(metruyenfull_parse, "make_request", fake_req)
    adapter = type("A", (), {"SITE_KEY": "metruyenfull"})()
    data = await metruyenfull_parse.get_story_metadata(adapter, "u")
    assert data["title"] == "Title"
    assert data["author"] == "Au"
    assert data["description"] == "Desc"
    assert data["categories"] == [{"name": "G", "url": "/g"}]
    assert data["total_chapters_on_site"] == 200
    assert data["cover"] == "c.jpg"
