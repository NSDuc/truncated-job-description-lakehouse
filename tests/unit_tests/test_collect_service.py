from unittest import TestCase
from job_desc_lakehouse.services.collect_service import JDCollectOptions, JDCollectServiceImpl


class TestJDCollectServiceImpl(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestJDCollectServiceImpl, self).__init__(*args, **kwargs)
        self.collect_impl = JDCollectServiceImpl()

    def test_scrape_page_number(self):
        page_num = self.collect_impl.scrape_page_number()
        print(f'number of pages is {page_num}')
        self.assertGreater(page_num, 0)

    def test_scrape_job_descriptions_in_page(self, page_number=1):
        jobs = self.collect_impl.scrape_job_descriptions_in_page(page_number)
        for job in jobs:
            print(job)

    def test_scrape_job_descriptions_in_multiple_pages(self):
        pages = [1, 2, 4]
        options = JDCollectOptions(50, None)
        jobs = self.collect_impl.scrape_job_descriptions_in_multiple_pages(pages, options)
        for i, job in enumerate(jobs):
            print(f'{i+1:2} > {job}')

    def test_scrape_job_descriptions(self):
        options = JDCollectOptions(15, 9353358)
        jobs = self.collect_impl.scrape_job_descriptions(options)
        for i, job in enumerate(jobs):
            print(f'{i+1:2} > {job}')
