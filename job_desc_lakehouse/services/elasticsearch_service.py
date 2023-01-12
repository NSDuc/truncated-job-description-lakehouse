from collections import Counter
from elasticsearch import Elasticsearch
from job_desc_lakehouse.DTO.job_desc import JobDescPropName

_vietnam_stopwords = ["bị", "bởi", "cả", "các", "cái", "cần", "càng", "chỉ", "chiếc", "cho", "chứ", "chưa", "chuyện",
                      "có", "có thể", "cứ", "của", "cùng", "cũng", "đã", "đang", "đây", "để", "đến nỗi", "đều", "điều",
                      "do", "đó", "được", "dưới", "gì", "khi", "không", "là", "lại", "lên", "lúc", "mà", "mỗi",
                      "một cách", "này", "nên", "nếu", "ngay", "nhiều", "như", "nhưng", "những", "nơi", "nữa", "phải",
                      "qua", "ra", "rằng", "rằng", "rất", "rất", "rồi", "sau", "sẽ", "so", "sự", "tại", "theo", "thì",
                      "trên", "trong", "trước", "từ", "từng", "và", "vẫn", "vào", "vậy", "vì", "việc", "với", "vừa"]


class _EsFieldType:
    @staticmethod
    def field(_type, **kwargs):
        conf = {"type": _type, **kwargs}
        return conf

    @staticmethod
    def subfield(_name, _type, **kwargs):
        conf = {
            _name: {"type": _type},
            **kwargs
        }
        return conf

    @staticmethod
    def LONG(**kwargs):
        return _EsFieldType.field("long", **kwargs)

    @staticmethod
    def KEYWORD(**kwargs):
        return _EsFieldType.field("keyword", **kwargs)

    @staticmethod
    def TEXT(**kwargs):
        return _EsFieldType.field("text", **kwargs)

    @staticmethod
    def TEXT_KEYWORD(**kwargs):
        return _EsFieldType.TEXT(**kwargs, fields=_EsFieldType.subfield("keyword", "keyword"))


class _EsAnalysisName:
    TAG_ANALYZER = "custom_tag_analyzer"
    SHINGLE_12_FILTER = "custom_shingle_12_filter"
    ENGLISH_STOP_FILTER = "custom_english_stop_filter"
    ENGLISH_STEM_FILTER = "custom_english_stem_filter"
    VIETNAM_STOP_FILTER = "custom_vietnam_stop_filter"
    NUMBER_STOP_FILTER = "custom_number_stop_filter"


class ElasticSearchServiceImpl:
    _SETTING_BODY_0 = {
        "mappings": {
            "properties": {
                JobDescPropName.SOURCE: _EsFieldType.KEYWORD(),
                JobDescPropName.COLLECT_ID: _EsFieldType.LONG(),
                JobDescPropName.TITLE: _EsFieldType.TEXT_KEYWORD(),
                JobDescPropName.TAGS: _EsFieldType.TEXT_KEYWORD(),
                JobDescPropName.COMPANY: _EsFieldType.TEXT_KEYWORD(),
                JobDescPropName.OVERVIEW: _EsFieldType.TEXT(),
                JobDescPropName.REQUIREMENT: _EsFieldType.TEXT(),
                JobDescPropName.BENEFIT: _EsFieldType.TEXT(),
            }
        }
    }

    _SETTING_BODY_1 = {
        "settings": {
            "analysis": {
                "analyzer": {
                    _EsAnalysisName.TAG_ANALYZER: {
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            _EsAnalysisName.ENGLISH_STOP_FILTER,
                            _EsAnalysisName.VIETNAM_STOP_FILTER,
                            _EsAnalysisName.SHINGLE_12_FILTER,
                            _EsAnalysisName.NUMBER_STOP_FILTER,
                            "trim",
                        ],
                    }
                },
                "filter": {
                    _EsAnalysisName.SHINGLE_12_FILTER: {
                        "type": "shingle",
                        "min_shingle_size": 2,
                        "max_shingle_size": 2,
                        "filler_token": "",
                    },
                    _EsAnalysisName.ENGLISH_STOP_FILTER: {
                        "type": "stop",
                        "stopwords": "_english_"
                    },
                    _EsAnalysisName.ENGLISH_STEM_FILTER: {
                        "type": "stemmer",
                        "language": "light_english"
                    },
                    _EsAnalysisName.VIETNAM_STOP_FILTER: {
                        "type": "stop",
                        "stopwords": _vietnam_stopwords
                    },
                    _EsAnalysisName.NUMBER_STOP_FILTER: {
                        "type": "pattern_replace",
                        "pattern": "^(0|[1-9][0-9]*)$",
                        "replacement": ""
                    },
                },
            }
        },
        "mappings": {
            "properties": {
                JobDescPropName.SOURCE: _EsFieldType.KEYWORD(),
                JobDescPropName.COLLECT_ID: _EsFieldType.LONG(),
                JobDescPropName.TITLE: _EsFieldType.TEXT_KEYWORD(),
                JobDescPropName.TAGS: _EsFieldType.TEXT_KEYWORD(fielddata=True),
                JobDescPropName.COMPANY: _EsFieldType.TEXT_KEYWORD(),
                JobDescPropName.OVERVIEW: _EsFieldType.TEXT(fielddata=True, analyzer=_EsAnalysisName.TAG_ANALYZER),
                JobDescPropName.REQUIREMENT: _EsFieldType.TEXT(fielddata=True, analyzer=_EsAnalysisName.TAG_ANALYZER),
                JobDescPropName.BENEFIT: _EsFieldType.TEXT(fielddata=True, analyzer=_EsAnalysisName.TAG_ANALYZER),
            },
        }
    }

    def __init__(self, hosts):
        self._client = Elasticsearch(hosts)

    def get_info(self):
        return self._client.info()

    def search(self, index, field, text):
        body = {
            "query": {
                "match": {
                    field: text
                }
            }
        }

        return self._client.search(index=index, body=body)

    def setup_index_0(self, index):
        return self._client.indices.create(
            index=index,
            body=ElasticSearchServiceImpl._SETTING_BODY_0
        )

    def setup_index_1(self, index):
        return self._client.indices.create(
            index=index,
            body=ElasticSearchServiceImpl._SETTING_BODY_1
        )

    def analyze_text(self, index, text=''):
        body = {
            "analyzer": _EsAnalysisName.TAG_ANALYZER,
            "text": text
        }
        return self._client.indices.analyze(index=index, body=body)

    def get_indices(self, name='*'):
        return self._client.indices.get(name)

    def delete_index(self, index):
        return self._client.indices.delete(index=index, ignore=[400, 404])

    def count(self, index=None):
        return self._client.count(index=index)

    def reindex(self, src_index=None, dst_index=None):
        return self._client.reindex(
            body={
                "source": {
                    "index": src_index,
                 },
                "dest": {
                    "index": dst_index
                }
            })

    def get_term_count(self, field, index, min_doc_count=1) -> Counter:
        agg_name = f"{field}_aggregation"
        body = {
            "size": 0,
            "aggs": {
                agg_name: {
                    "terms": {
                        "field": field,
                        "size": 10000,
                        "min_doc_count": min_doc_count,
                    }
                }
            }
        }

        resp = self._client.search(index=index, body=body)
        buckets = resp["aggregations"][agg_name]["buckets"]
        term_count = Counter()
        for bucket in buckets:
            key = bucket["key"]
            val = bucket["doc_count"]
            term_count[key] = val
        return term_count
