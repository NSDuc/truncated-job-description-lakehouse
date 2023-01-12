import json
from typing import List
from unittest import TestCase
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

from test_base import TestContext
from job_desc_lakehouse.services.collect_service import JDCollectOptions, JDCollectServiceImpl
from job_desc_lakehouse.services.kafka_service import KafkaProducerImpl, KafkaAdminClientImpl
from job_desc_lakehouse.services.storage_stage_service import StorageStageServiceImpl
from job_desc_lakehouse.services.spark_service import SparkServiceImpl
from job_desc_lakehouse.DTO.job_desc import JobDesc
from job_desc_lakehouse.DTO.job_desc_builder import ItViecJobDescBuilder


class TestIntegrationStageContext(TestContext):
    def __init__(self):
        super(TestIntegrationStageContext, self).__init__()


class TestIntegrationStage(TestCase, TestIntegrationStageContext):
    def __init__(self, *args, **kwargs):
        TestIntegrationStageContext.__init__(self)
        TestCase.__init__(self, *args, **kwargs)

    def test_integration_stage(self):
        # kafka_admin = KafkaAdminClientImpl(bootstrap_servers=self.kafka_bootstrap_servers)
        # kafka_admin.delete_topic(self.kafka_topic)

        kafka_producer = KafkaProducerImpl(bootstrap_servers=self.kafka_bootstrap_servers,
                                           topic=self.kafka_topic)
        kafka_consumer = KafkaConsumer(self.kafka_topic,
                                       bootstrap_servers=self.kafka_bootstrap_servers,
                                       auto_offset_reset='earliest',
                                       enable_auto_commit=True,
                                       value_deserializer=lambda x: json.loads(x.decode('utf-8')))

        job_collect_options = JDCollectOptions(
            max_job_collect_per_task=15,
            last_collect_job_id=None
        )
        job_collector = JDCollectServiceImpl()

        jobs : List[JobDesc] = job_collector.collect_job_descriptions(job_collect_options)
        for i, job in enumerate(jobs):
            print(f'{i:2} | {job}')
            kafka_producer.send(message=job.raw,
                                headers=[('source', job.source.encode()),
                                         ('collect_id', '1'.encode())],
                                key=job.source.encode())

        builder = ItViecJobDescBuilder()
        for i, send_job in enumerate(jobs):
            record: ConsumerRecord = next(kafka_consumer)

            builder.reset_builder()
            builder.set_raw(record.value)
            recv_job = builder.build_job_desc_dto()

            print(f'offset={record.offset:2} | {recv_job}')
            self.assertEqual(send_job.source, record.headers[0][1].decode())
            self.assertEqual(send_job.__str__(), recv_job.__str__())
            self.assertEqual(send_job.id, recv_job.id)

        kafka_consumer.close()

    def test_kafka_data(self):
        spark_conf = {
            "spark.sql.shuffle.partitions": 1,
            "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,"
        }
        spark_impl = SparkServiceImpl(app_name='test_kafka', config=spark_conf)
        df = spark_impl.readstream_dataframe_from_kafka(self.kafka_bootstrap_servers, self.kafka_topic)
        spark_impl.writestream_dataframe_to_console(df, 'kafka_raw_data')
        df.printSchema()

        df2 = df.selectExpr('CAST(value AS STRING)')
        spark_impl.writestream_dataframe_to_console(df2, 'kafka value as string')

    def test_storage_stage(self):
        spark_impl = SparkServiceImpl.Builder.create_spark_impl_for_delta_kafka('test_storage')

        storage_impl = StorageStageServiceImpl(spark_impl=spark_impl,
                                               kafka_bootstrap_servers=self.kafka_bootstrap_servers,
                                               kafka_topic=self.kafka_topic,
                                               storage_path=self.storage_path)
        storage_impl.run_etl()

