import time
from os.path import join
from unittest import TestCase
from test_base import TestContext
from job_desc_lakehouse.services.processing_stage_service import ProcessingStageServiceImpl
from job_desc_lakehouse.services.spark_service import SparkServiceImpl


class TestProcessingStageContext(TestContext):
    def __init__(self):
        super(TestProcessingStageContext, self).__init__()


class TestProcessingStage(TestCase, TestProcessingStageContext):
    def __init__(self, *args, **kwargs):
        TestProcessingStageContext.__init__(self)
        TestCase.__init__(self, *args, **kwargs)
        self.spark_impl = SparkServiceImpl.Builder.create_spark_impl_for_delta_elasticsearch(app_name='test_processing')

    def test_processing_stage(self):
        process_impl = ProcessingStageServiceImpl(self.spark_impl,
                                                  storage_path=self.storage_path,
                                                  process_path=self.process_path,
                                                  analysis_path=self.analysis_path,
                                                  elasticsearch_index=self.elasticsearch_index_0,
                                                  elasticsearch_nodes=self.elasticsearch_hosts,
                                                  elasticsearch_checkpoint=self.elasticsearch_checkpoint)

        process_impl.run_etl()
        time.sleep(30)

    def test_processing_stage_data(self):
        self.spark_impl.show_medallion_table_info(path=self.process_path)
        self.spark_impl.show_medallion_table_info(path=join(self.analysis_path, 'tags'))
