import json
import time
from collections import Counter
from os.path import join
from typing import List
from unittest import TestCase
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

from test_common import *
from job_desc_lakehouse.services.collect_service import JDCollectOptions, JDCollectServiceImpl
from job_desc_lakehouse.services.kafka_service import KafkaProducerImpl, KafkaAdminClientImpl
from job_desc_lakehouse.services.storage_stage_service import StorageStageServiceImpl
from job_desc_lakehouse.services.processing_stage_service import ProcessingStageServiceImpl
from job_desc_lakehouse.services.analysis_stage_service import AnalysisStageServiceImpl
from job_desc_lakehouse.services.tag_checker_service import TagChecker
from job_desc_lakehouse.services.spark_service import SparkServiceImpl
from job_desc_lakehouse.services.elasticsearch_service import ElasticSearchServiceImpl
from job_desc_lakehouse.services.wordcloud_service import WordCloudServiceImpl
from job_desc_lakehouse.DTO.job_desc import JobDesc, JobDescPropName
from job_desc_lakehouse.DTO.job_desc_builder import ItViecJobDescBuilder


class TestJobDescriptionPipeline(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestJobDescriptionPipeline, self).__init__(*args, **kwargs)
        conf = TestConfigLoader.load_config()
        self.kafka_bootstrap_servers = conf[KAFKA_BOOTSTRAP_SERVERS]
        self.kafka_topic = conf[KAFKA_TOPIC]

        self.storage_path = conf[SPARK_STORAGE_PATH]
        self.process_path = conf[SPARK_PROCESS_PATH]
        self.analysis_path = conf[SPARK_ANALYSIS_PATH]

        self.delta_word_cloud_path = conf[DELTA_WORD_CLOUD_PATH]
        self.analyzed_word_cloud_path_1 = conf[ANALYZED_WORD_CLOUD_PATH_1]

        self.elasticsearch_hosts = conf[ELASTICSEARCH_HOSTS]
        self.elasticsearch_index_0 = conf[ELASTICSEARCH_INDEX_0]
        self.elasticsearch_index_1 = conf[ELASTICSEARCH_INDEX_1]
        self.elasticsearch_checkpoint = conf[SPARK_ELASTICSEARCH_CHECKPOINT]
        self.es_impl = ElasticSearchServiceImpl(hosts=self.elasticsearch_hosts)

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
        storage_impl.run()

    def test_storage_stage_data(self):
        spark_impl = SparkServiceImpl.Builder.create_spark_impl_for_delta('test_bronze')
        spark_impl.show_medallion_table_info(path=self.storage_path)

    def test_processing_stage(self):
        spark_impl = SparkServiceImpl.Builder.create_spark_impl_for_delta_elasticsearch(app_name='test_processing')

        process_impl = ProcessingStageServiceImpl(spark_impl,
                                                  storage_path=self.storage_path,
                                                  process_path=self.process_path,
                                                  analysis_path=self.analysis_path,
                                                  elasticsearch_index=self.elasticsearch_index_0,
                                                  elasticsearch_nodes=self.elasticsearch_hosts,
                                                  elasticsearch_checkpoint=self.elasticsearch_checkpoint)

        process_impl.run()
        time.sleep(30)

    def test_processing_stage_data(self):
        spark_impl = SparkServiceImpl.Builder.create_spark_impl_for_delta(app_name='test_silver')
        spark_impl.show_medallion_table_info(path=self.process_path)
        spark_impl.show_medallion_table_info(path=join(self.analysis_path, 'tags'))

    def test_analysis_stage(self):
        spark_impl = SparkServiceImpl.Builder.create_spark_impl_for_delta(app_name='test_analysis')
        analysis_impl = AnalysisStageServiceImpl(spark_impl,
                                                 process_path=self.process_path,
                                                 analysis_path=self.analysis_path,
                                                 cloud_word_path=self.delta_word_cloud_path)
        analysis_impl.run()

    def test_analysis_stage_delta_data(self):
        spark_impl = SparkServiceImpl.Builder.create_spark_impl_for_delta(app_name='test_gold')
        spark_impl.show_medallion_table_info(path=join(self.analysis_path, 'tags'))

        WordCloudServiceImpl.open_word_cloud(self.delta_word_cloud_path)

    def test_analysis_stage_data(self):
        tag_term_count = None
        total_term_point = Counter()
        field_params = {
            JobDescPropName.TAGS: (6, 1),
            JobDescPropName.OVERVIEW: (1, 3),
            JobDescPropName.REQUIREMENT: (1, 3),
            JobDescPropName.BENEFIT: (1, 3),
        }
        for field, (point, min_doc_count) in field_params.items():
            term_point = self.es_impl.get_term_count(field=field,
                                                     index=self.elasticsearch_index_1,
                                                     min_doc_count=min_doc_count)
            if field == JobDescPropName.TAGS:
                tag_term_count = term_point

            for i in range(point):
                total_term_point += term_point.copy()
        self.assertIsNotNone(tag_term_count)
        tags, not_tags = TagChecker.check(total_term_point, path="../docs/tags.txt")

        tags_point = {k: v for k, v in total_term_point.items() if k in tags or k in tag_term_count}
        not_tags_point = {k: v for k, v in total_term_point.items() if k in not_tags and k not in tag_term_count}

        WordCloudServiceImpl.generate_table(tags_point)
        WordCloudServiceImpl.generate_word_cloud(frequency=tags_point, savefig_path=self.analyzed_word_cloud_path_1)
