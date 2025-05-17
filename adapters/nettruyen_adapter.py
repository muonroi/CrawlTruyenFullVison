from adapters.base_site_adapter import BaseSiteAdapter


class NetTruyenAdapter(BaseSiteAdapter):
    def get_genres(self):
        # code crawl genre của nettruyen
        pass

    def get_stories_in_genre(self, genre_url, page=1):
        # code crawl list stories của nettruyen
        pass

    def get_story_details(self, story_url):
        # code crawl chi tiết truyện
        pass

    def get_chapter_list(self, story_url):
        # code crawl danh sách chương
        pass

    def get_chapter_content(self, chapter_url):
        # code crawl nội dung chương
        pass