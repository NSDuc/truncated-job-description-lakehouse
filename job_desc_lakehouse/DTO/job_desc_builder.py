import hashlib
from bs4 import BeautifulSoup
from job_desc_lakehouse.DTO.job_desc import JobDesc


class JobDescBuilder:
    def __init__(self):
        self._id           = None
        self._source       = None
        self._title        = None
        self._tags         = None
        self._company      = None
        self._overview     = None
        self._requirement  = None
        self._benefit      = None
        self._post_time    = None
        self._collect_time = None
        self._raw          = None

    def set_source(self, source):
        self._source = source
        return self

    def set_title(self, title):
        self._title = title
        return self

    def set_tags(self, tags):
        self._tags = tags
        return self

    def set_company(self, company):
        self._company = company
        return self

    def set_overview(self, overview):
        self._overview = overview
        return self

    def set_requirement(self, requirement):
        self._requirement = requirement
        return self

    def set_benefit(self, benefit):
        self._benefit = benefit
        return self

    def set_post_time(self, post_time):
        self._post_time = post_time
        return self

    def set_collect_time(self, collect_time):
        self._collect_time = collect_time
        return self

    def set_raw(self, raw):
        self._raw = raw
        return self

    def set_id(self, id):
        self._id = id
        return self

    def reset_builder(self):
        self._id = None
        self._source = None
        self._title = None
        self._tags = None
        self._company = None
        self._overview = None
        self._requirement = None
        self._benefit = None
        self._post_time = None
        self._raw = None

    def build_job_desc_dto(self) -> JobDesc:
        job_desc = JobDesc()
        job_desc.id           = self._id
        job_desc.source       = self._source
        job_desc.title        = self._title
        job_desc.tags         = self._tags
        job_desc.company      = self._company
        job_desc.overview     = self._overview
        job_desc.requirement  = self._requirement
        job_desc.benefit      = self._benefit
        job_desc.post_time    = self._post_time
        job_desc.collect_time = self._collect_time
        job_desc.raw          = self._raw
        self.reset_builder()
        return job_desc


class ItViecJobDescBuilder(JobDescBuilder):
    def build_job_desc_id(self):
        hash_text = ""
        try:
            hash_text = self._title[:10] + self._company[:10] + self._requirement[:10] \
                        + self._benefit[:10] + self._overview[:10]
            self.set_id(int(hashlib.sha1(hash_text.encode("utf-8")).hexdigest(), 16) % (10 ** 8))
        except Exception as e:
            print(hash_text)
            self.set_id(0)

    def build_job_desc_from_raw_html(self):
        bs         = BeautifulSoup(self._raw, features='html.parser')
        tags       = bs.find(class_='job-details__tag-list')
        title      = bs.find(class_='job-details__title')
        company    = bs.find(class_='job-details__sub-title')
        paragraphs = bs.find_all(class_='job-details__paragraph')
        paragraphs_ = ['', '', '']
        for i, p in enumerate(paragraphs):
            paragraphs_[i] = p.text
        self.set_title(title.text if title else '')
        self.set_tags([tag.text for tag in tags.children] if tags else [])
        self.set_company(company.text if company else '')
        self.set_benefit(paragraphs_[2])
        self.set_overview(paragraphs_[0])
        self.set_requirement(paragraphs_[1])

    def build_job_desc_dto(self) -> JobDesc:
        self.set_source("itviec.com")
        self.build_job_desc_from_raw_html()
        # self.build_job_desc_id()
        return super(ItViecJobDescBuilder, self).build_job_desc_dto()


class VietNamWorkJobDescBuilder(JobDescBuilder):
    def build_job_desc_dto(self) -> JobDesc:
        raise NotImplementedError
