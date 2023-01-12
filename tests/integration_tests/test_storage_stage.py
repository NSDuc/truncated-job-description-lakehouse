from unittest import TestCase
from test_base import TestContext
from job_desc_lakehouse.services.storage_stage_service import StorageStageServiceImpl
from job_desc_lakehouse.services.spark_service import SparkServiceImpl


class TestStorageStageContext(TestContext):
    def __init__(self):
        super(TestStorageStageContext, self).__init__()


class TestStorageStage(TestCase, TestStorageStageContext):
    def __init__(self, *args, **kwargs):
        TestStorageStageContext.__init__(self)
        TestCase.__init__(self, *args, **kwargs)

    def test_storage_stage(self):
        spark_impl = SparkServiceImpl.Builder.create_spark_impl_for_delta_kafka('test_storage')

        storage_impl = StorageStageServiceImpl(spark_impl=spark_impl,
                                               kafka_bootstrap_servers=self.kafka_bootstrap_servers,
                                               kafka_topic=self.kafka_topic,
                                               storage_path=self.storage_path)
        storage_impl.run_etl()

    def test_storage_stage_data(self):
        spark_impl = SparkServiceImpl.Builder.create_spark_impl_for_delta('test_bronze')
        spark_impl.show_medallion_table_info(path=self.storage_path)
