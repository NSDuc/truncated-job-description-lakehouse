from collections import Counter
from test_base import TestContext
from pprint import pprint
from unittest import TestCase
from job_desc_lakehouse.services.elasticsearch_service import ElasticSearchServiceImpl
from job_desc_lakehouse.DTO.job_desc import JobDescPropName
from job_desc_lakehouse.services.wordcloud_service import WordCloudServiceImpl


class TestElasticsearchServiceContext(TestContext):
    def __init__(self):
        super(TestElasticsearchServiceContext, self).__init__()
        self.es_impl = ElasticSearchServiceImpl(hosts=self.elasticsearch_hosts)


class TestElasticsearchServiceCommon(TestCase, TestElasticsearchServiceContext):
    def __init__(self, *args, **kwargs):
        TestElasticsearchServiceContext.__init__(self)
        TestCase.__init__(self, *args, **kwargs)

    def test_get_info(self):
        resp = self.es_impl.get_info()
        self.assertIsInstance(resp, dict)
        print(f'elasticsearch info:')
        pprint(resp)

    def test_get_indices(self):
        indices = self.es_impl.get_indices()
        print(f"indices: {[ind for ind in indices.keys() if not ind.startswith('.')]}")

        for index in [self.elasticsearch_index_0, self.elasticsearch_index_1]:
            resp = self.es_impl.get_indices(self.elasticsearch_index_1)
            print(f"index {index} info:")
            pprint(resp)

    def test_search(self):
        text = "k8s C#"
        pprint(self.es_impl.search(self.elasticsearch_index_1, JobDescPropName.REQUIREMENT, text))


class TestElasticsearchIndex0(TestCase, TestElasticsearchServiceContext):
    def __init__(self, *args, **kwargs):
        TestElasticsearchServiceContext.__init__(self)
        TestCase.__init__(self, *args, **kwargs)

    def test_delete_index(self):
        resp = self.es_impl.delete_index(index=self.elasticsearch_index_0)
        print(f'delete index: {resp}')

    def test_setup(self):
        resp = self.es_impl.setup_index_0(self.elasticsearch_index_0)
        pprint(resp)

    def test_count(self):
        pprint(self.es_impl.count(self.elasticsearch_index_0))


class TestElasticsearchIndex1(TestCase, TestElasticsearchServiceContext):
    def __init__(self, *args, **kwargs):
        TestElasticsearchServiceContext.__init__(self)
        TestCase.__init__(self, *args, **kwargs)

    def test_delete_index(self):
        resp = self.es_impl.delete_index(index=self.elasticsearch_index_1)
        print(f'delete index: {resp}')

    def test_setup(self):
        pprint(self.es_impl.setup_index_1(self.elasticsearch_index_1))

    def test_count(self):
        pprint(self.es_impl.count(self.elasticsearch_index_1))

    def test_analyzer_text(self):
        index = "test_job_description_analyzer"
        self.es_impl.setup_index_1(index)
        text = "Welcome to Việt Nam; The quick brown wait a second or 2 seconds k8s apache"
        resp = self.es_impl.analyze_text(index, text)

        for token in resp['tokens']:
            print(f"<{token['token']}>")

        self.es_impl.delete_index(index)


class TestElasticsearchServiceImpl(TestCase, TestElasticsearchServiceContext):
    def __init__(self, *args, **kwargs):
        TestElasticsearchServiceContext.__init__(self)
        TestCase.__init__(self, *args, **kwargs)

    def test_reindex(self):
        src_index = self.elasticsearch_index_0
        dst_index = self.elasticsearch_index_1
        resp = self.es_impl.reindex(src_index, dst_index)
        pprint(resp)

    def test_delete_reindex(self):
        src_index = self.elasticsearch_index_0
        dst_index = self.elasticsearch_index_1

        pprint(self.es_impl.delete_index(dst_index))
        pprint(self.es_impl.setup_index_1(dst_index))
        pprint(self.es_impl.reindex(src_index, dst_index))
        pprint(self.es_impl.count(dst_index))

    def test_term_count_0(self):
        term_count = self.es_impl.get_term_count(field=JobDescPropName.TAGS,
                                                 index=self.elasticsearch_index_1)
        WordCloudServiceImpl.generate_table(term_count)
        WordCloudServiceImpl.generate_word_cloud(frequency=term_count, savefig_path=self.analyzed_word_cloud_path_0)

    def test_term_count_1(self):
        total_term_count = Counter()
        field_params = {
            JobDescPropName.TAGS: (6, 1),
            JobDescPropName.OVERVIEW: (1, 10),
            JobDescPropName.REQUIREMENT: (1, 10),
            JobDescPropName.BENEFIT: (1, 10),
        }
        for field, (point, min_doc_count) in field_params.items():
            term_count = self.es_impl.get_term_count(field=field,
                                                     index=self.elasticsearch_index_1,
                                                     min_doc_count=min_doc_count)
            for i in range(point):
                total_term_count += term_count.copy()
                print(f"{field} point {point}")

        WordCloudServiceImpl.generate_table(total_term_count)
        WordCloudServiceImpl.generate_word_cloud(frequency=total_term_count)
