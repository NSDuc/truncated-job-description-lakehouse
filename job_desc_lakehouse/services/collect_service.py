import concurrent.futures
import os
import pendulum
from typing import List
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from job_desc_lakehouse.DTO.job_desc_dto import JobDescriptionDTO
from job_desc_lakehouse.DTO.job_desc_dto_builder import ItViecJobDescDTOBuilder


class JDCollectOptions:
    def __init__(self, max_job_collect_per_task=None, last_collect_job_id=None):
        self._max_job_collect_per_task = max_job_collect_per_task
        self._last_collected_job_id = last_collect_job_id

    def last_collected_job_id(self):
        return self._last_collected_job_id

    def max_job_collect_per_task(self):
        return self._max_job_collect_per_task


class JDCollectService:
    def __init__(self):
        self.job_desc_dto_builder = None

    def collect_job_descriptions(self, options: JDCollectOptions) -> List[JobDescriptionDTO]:
        pass


class JDCollectServiceImpl(JDCollectService):
    def __init__(self):
        super(JDCollectServiceImpl, self).__init__()
        self.job_desc_dto_builder = ItViecJobDescDTOBuilder()
        self.firefox_options = Options()
        self.firefox_options.headless = True

    def get_firefox_driver(self) -> webdriver.Firefox:
        # Work for Mozilla Firefox 101.0.1
        # NOT Work for Mozilla Firefox 59.x.x
        return webdriver.Firefox(options=self.firefox_options, service_log_path=os.devnull)

    def scrape_page_number(self) -> int:
        with self.get_firefox_driver() as driver:
            driver.get('https://itviec.com/it-jobs')
            page_control_elm = driver.find_element(By.CLASS_NAME, 'search-page__jobs-pagination')
            page_li_elm_list = page_control_elm.find_elements(By.TAG_NAME, 'li')

            page_num = max([int(elm.text) for elm in page_li_elm_list if elm.text.isdigit()])
        return page_num

    def scrape_job_descriptions(self, options: JDCollectOptions) -> List[JobDescriptionDTO]:
        if options.max_job_collect_per_task():
            # optimize runtime: 20 is number of jobs per page
            pages = range(1, options.max_job_collect_per_task()//20 + 2)
        else:
            pages = range(1, self.scrape_page_number() + 1)

        print(f'collect page range is 1..{pages[-1]}')

        jobs = self.scrape_job_descriptions_in_multiple_pages(pages=pages, options=options)
        if options.last_collected_job_id() and len(jobs):
            if jobs[-1].id == options.last_collected_job_id():
                del jobs[-1]  # remove job which is collected in previous run
        return jobs

    def scrape_job_descriptions_in_page(self, page, last_job_id=None, max_job_num=None) -> List[JobDescriptionDTO]:
        job_desc_dto_builder = ItViecJobDescDTOBuilder()
        jobs = []

        with self.get_firefox_driver() as driver:
            driver.get(f'https://itviec.com/it-jobs?page={page}&query=&source=search_job')

            first_grp_elm = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.CLASS_NAME, 'first-group')))
            job_elm_list = first_grp_elm.find_elements(By.CLASS_NAME, 'job')

            prv_jd_elm = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.CLASS_NAME, 'job-details')))

            for i, elm in enumerate(job_elm_list):
                driver.execute_script("arguments[0].click();", elm)
                WebDriverWait(driver, 10).until(EC.staleness_of(prv_jd_elm))

                job_elm = driver.find_element(By.CLASS_NAME, 'job-details')
                job_raw_data = job_elm.get_attribute('innerHTML').replace('\n', '')

                job_desc_dto_builder.set_raw(job_raw_data)
                job_desc_dto_builder.set_collect_time(pendulum.now().to_datetime_string())

                job = job_desc_dto_builder.build_job_desc_dto()
                if '' in [job.requirement, job.overview, job.benefit]:
                    print(f'detect abnormal html index [{i}/{page}]')

                jobs.append(job)

                if job.id == last_job_id or len(jobs) == max_job_num:
                    return jobs
                prv_jd_elm = job_elm

        return jobs

    def scrape_job_descriptions_in_multiple_pages(self, pages, options: JDCollectOptions) -> List[JobDescriptionDTO]:
        jobs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            jobs_groups = executor.map(self.scrape_job_descriptions_in_page, pages)
            for jg in jobs_groups:
                jobs.extend(jg)

        if options.max_job_collect_per_task() and (len(jobs) > options.max_job_collect_per_task()):
            jobs = jobs[:options.max_job_collect_per_task()]
        if options.last_collected_job_id():
            for i, job in enumerate(jobs):
                if job.id == options.last_collected_job_id():
                    jobs = jobs[:i+1]
                    break
        return jobs

    def collect_job_descriptions(self, options: JDCollectOptions) -> List[JobDescriptionDTO]:
        return self.scrape_job_descriptions(options)
