
import unittest
import os
from analyze.xtruyen_parse import parse_story_info, parse_chapter_content

class TestXTruyenParse(unittest.TestCase):

    def setUp(self):
        self.test_data_path = os.path.join(os.path.dirname(__file__), '..', 'tmp_data', 'xtruyen')
        self.detail_story_html = self._read_test_file('detail_story.txt')
        self.content_chapter_html = self._read_test_file('content_chapter.txt')

    def _read_test_file(self, filename):
        try:
            with open(os.path.join(self.test_data_path, filename), 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            # Fallback for different execution context
            alt_path = os.path.join(os.path.dirname(__file__), '../', 'tmp_data', 'xtruyen', filename)
            with open(alt_path, 'r', encoding='utf-8') as f:
                return f.read()

    def test_parse_story_info(self):
        self.assertIsNotNone(self.detail_story_html, "detail_story.txt not found")
        story_info = parse_story_info(self.detail_story_html)
        
        self.assertEqual(story_info['title'], 'THỜI BUỔI NÀY AI CÒN ĐƯƠNG ĐỨNG ĐẮN HỒ YÊU')
        self.assertEqual(story_info['author'], 'Ngã Lai Tạng Ba Binh Tuyến')
        self.assertTrue(story_info['description'].startswith('Ta kêu Trần Nguyên'))
        self.assertEqual(story_info['post_id'], '10235224')

    def test_parse_chapter_content(self):
        self.assertIsNotNone(self.content_chapter_html, "content_chapter.txt not found")
        chapter_content = parse_chapter_content(self.content_chapter_html)
        
        self.assertEqual(chapter_content['title'], 'Chương 1: hồ gia tiên sư')
        self.assertIsNotNone(chapter_content['content'])
        self.assertIn('<p>', chapter_content['content'], "Decompressed content should contain HTML tags")
        self.assertIn('Nửa đêm canh ba', chapter_content['content'], "Decompressed content seems incorrect")

if __name__ == '__main__':
    unittest.main()
