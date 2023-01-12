from collections import Counter
from os.path import join
from unittest import TestCase
from test_base import TestContext
from job_desc_lakehouse.services.analysis_stage_service import AnalysisStageServiceImpl
from job_desc_lakehouse.services.tag_checker_service import TagChecker
from job_desc_lakehouse.services.spark_service import SparkServiceImpl
from job_desc_lakehouse.services.elasticsearch_service import ElasticSearchServiceImpl
from job_desc_lakehouse.services.wordcloud_service import WordCloudServiceImpl
from job_desc_lakehouse.DTO.job_desc import JobDescPropName


class TestAnalysisStageContext(TestContext):
    def __init__(self):
        super(TestAnalysisStageContext, self).__init__()


class TestDeltaAnalysisStage(TestCase, TestAnalysisStageContext):
    def __init__(self, *args, **kwargs):
        TestAnalysisStageContext.__init__(self)
        TestCase.__init__(self, *args, **kwargs)
        self.spark_impl = SparkServiceImpl.Builder.create_spark_impl_for_delta(app_name='test_analysis')

    def test_analysis_stage(self):
        analysis_impl = AnalysisStageServiceImpl(self.spark_impl,
                                                 process_path=self.process_path,
                                                 analysis_path=self.analysis_path,
                                                 cloud_word_path=self.delta_word_cloud_path)
        analysis_impl.run_etl()

    def test_analysis_stage_delta_data(self):
        self.spark_impl.show_medallion_table_info(path=join(self.analysis_path, 'tags'))


class TestAnalysisStage(TestCase, TestAnalysisStageContext):
    def __init__(self, *args, **kwargs):
        TestAnalysisStageContext.__init__(self)
        TestCase.__init__(self, *args, **kwargs)
        self.elasticsearch_impl = ElasticSearchServiceImpl(hosts=self.elasticsearch_hosts)

    def test_elasticsearch_document_count(self):
        resp = self.elasticsearch_impl.count(self.elasticsearch_index_0)
        self.assertGreater(resp['count'], 0, 'No document is indexed')
        resp = self.elasticsearch_impl.count(self.elasticsearch_index_1)
        self.assertGreater(resp['count'], 0, 'No document is indexed')

    def test_word_cloud(self):
        WordCloudServiceImpl.open_word_cloud(self.analyzed_word_cloud_path_0)
        WordCloudServiceImpl.open_word_cloud(self.analyzed_word_cloud_path_1)
        WordCloudServiceImpl.open_word_cloud(self.delta_word_cloud_path)

    def test_aggregation(self):
        tag_term_count = None
        total_term_point = Counter()
        field_params = {
            JobDescPropName.TAGS: (6, 1),
            JobDescPropName.OVERVIEW: (1, 3),
            JobDescPropName.REQUIREMENT: (1, 3),
            JobDescPropName.BENEFIT: (1, 3),
        }
        for field, (point, min_doc_count) in field_params.items():
            term_point = self.elasticsearch_impl.get_term_count(field=field,
                                                                index=self.elasticsearch_index_1,
                                                                min_doc_count=min_doc_count)
            if field == JobDescPropName.TAGS:
                tag_term_count = term_point

            for i in range(point):
                total_term_point += term_point.copy()
        self.assertIsNotNone(tag_term_count)
        tags, not_tags = TagChecker.check(total_term_point, path=self.tag_file_path)

        tags_point = {k: v for k, v in total_term_point.items() if k in tags or k in tag_term_count}
        not_tags_point = {k: v for k, v in total_term_point.items() if k in not_tags and k not in tag_term_count}

        WordCloudServiceImpl.generate_table(tags_point)
        WordCloudServiceImpl.generate_table(not_tags_point)
        WordCloudServiceImpl.generate_word_cloud(frequency=tags_point, savefig_path=self.analyzed_word_cloud_path_1)
