import collections
import random
from unittest import TestCase
from test_base import TestContext
from job_desc_lakehouse.services.wordcloud_service import WordCloudServiceImpl


class TestWordCloudServiceContext(TestContext):
    def __init__(self):
        super(TestWordCloudServiceContext, self).__init__()


class TestWordCloudServiceImpl(TestCase, TestWordCloudServiceContext):
    def __init__(self, *args, **kwargs):
        TestWordCloudServiceContext.__init__(self)
        TestCase.__init__(self, *args, **kwargs)

    def test_generate_word_cloud(self):
        frequency = collections.defaultdict(lambda: 0)
        text = ""
        for i in range(15):
            for char in range(ord('a'), ord('z') + 1):
                token = chr(char) * random.randint(1, 5)
                frequency[token] += 1
                text += token + " "
        key, max_val = (max(frequency.items(), key=lambda _k: _k[1]))
        print(f'{key:5}:{max_val}')
        print('-------------------')
        for k, v in frequency.items():
            print(f'{k:5}:{v}')

        WordCloudServiceImpl.generate_word_cloud(frequency=frequency)

    def test_open_word_cloud(self):
        WordCloudServiceImpl.open_word_cloud(self.analyzed_word_cloud_path_1)
