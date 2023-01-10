class JobDescriptionColumn:
    ID = 'id'
    TITLE = 'title'
    TAG = 'tag'
    TAGS = 'tags'
    COMPANY = 'company'
    OVERVIEW = 'overview'
    REQUIREMENT = 'requirement'
    BENEFIT = 'benefit'
    POST_TIME = 'post_time'
    COLLECT_ID = 'collect_id'
    SOURCE = 'source'


class JobDescriptionDTO:
    def __init__(self, id=None):
        self._id = id
        self._source: str = ''
        self._title = None
        self._tags = None
        self._company = None
        self._overview = None
        self._requirement = None
        self._benefit = None
        self._post_time = None
        self._collect_time = None
        self._raw = None

    @property
    def source(self): return self._source

    @source.setter
    def source(self, value):
        self._source = value

    @property
    def id(self): return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def title(self): return self._title

    @title.setter
    def title(self, value):
        self._title = value

    @property
    def tags(self): return self._tags

    @tags.setter
    def tags(self, value):
        self._tags = value

    @property
    def company(self): return self._company

    @company.setter
    def company(self, value):
        self._company = value

    @property
    def overview(self): return self._overview

    @overview.setter
    def overview(self, value):
        self._overview = value

    @property
    def requirement(self): return self._requirement

    @requirement.setter
    def requirement(self, value):
        self._requirement = value

    @property
    def benefit(self): return self._benefit

    @benefit.setter
    def benefit(self, value):
        self._benefit = value

    @property
    def post_time(self): return self._post_time

    @post_time.setter
    def post_time(self, value):
        self._post_time = value

    @property
    def collect_time(self): return self._collect_time

    @collect_time.setter
    def collect_time(self, value):
        self._collect_time = value

    @property
    def raw(self): return self._raw

    @raw.setter
    def raw(self, value):
        self._raw = value

    def __str__(self):
        return f'{self.id:8} | {self.title[:40]:40} | {self.company[:20]:20}'
