import json
from os.path import join
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from job_desc_lakehouse.DTO.job_desc_dto import JobDescriptionColumn
from job_desc_lakehouse.DTO.job_desc_dto_builder import ItViecJobDescDTOBuilder
from job_desc_lakehouse.services.etl_service import ETLService
from job_desc_lakehouse.services.spark_service import SparkServiceImpl
from job_desc_lakehouse.DTO.medalion_table_dto import MedallionTableDTO


class ProcessingStageServiceImpl(ETLService):
    def __init__(self, spark_iml: SparkServiceImpl, **kwargs):
        self.spark_impl: SparkServiceImpl = spark_iml
        self.bronze_table = MedallionTableDTO(base_path=kwargs['storage_path'])
        self.silver_table = MedallionTableDTO(base_path=kwargs['process_path'])
        gold_base_path = kwargs['analysis_path']
        self.gold_tags_table = MedallionTableDTO(join(gold_base_path, 'tags'))
        self.tag_stat_df = None

        self.es_index = kwargs['elasticsearch_index']
        self.es_nodes = kwargs['elasticsearch_nodes']
        self.es_checkpoint = kwargs['elasticsearch_checkpoint']

        self.job_description_schema = StructType([
            StructField(JobDescriptionColumn.TITLE, StringType(), False),
            StructField(JobDescriptionColumn.TAGS, ArrayType(StringType(), False), False),
            StructField(JobDescriptionColumn.OVERVIEW, StringType(), False),
            StructField(JobDescriptionColumn.REQUIREMENT, StringType(), False),
            StructField(JobDescriptionColumn.BENEFIT, StringType(), False),
            StructField(JobDescriptionColumn.COMPANY, StringType(), False),
        ])

    def transform_to_tag_stat_dataframe(self, df: DataFrame):
        tag_stat_df = df.select(JobDescriptionColumn.TAGS)\
            .withColumn(JobDescriptionColumn.TAG, explode(JobDescriptionColumn.TAGS)) \
            .groupby(JobDescriptionColumn.TAG).count() \
            .withColumnRenamed('count', 'tag_post_count')
        return tag_stat_df

    def transform_to_company_dataframe(self, df):
        company_df = df.groupby(JobDescriptionColumn.COMPANY).count() \
            .withColumnRenamed('count', 'company_post_count')
        return company_df

    def exact(self):
        return self.spark_impl.readstream_dataframe_from_medallion_table(self.bronze_table)

    def transform(self, df: DataFrame) -> DataFrame:
        def job_desc_builder_func(html):
            # Spark call function with html is None
            if html is None:
                return
            # Read raw-string with escape character
            html = bytes(html, 'utf-8').decode("unicode_escape")

            builder = ItViecJobDescDTOBuilder()
            builder.set_raw(html)

            job = builder.build_job_desc_dto()
            return json.dumps({
                JobDescriptionColumn.TITLE: job.title,
                JobDescriptionColumn.TAGS: job.tags,
                JobDescriptionColumn.COMPANY: job.company,
                JobDescriptionColumn.OVERVIEW: job.overview,
                JobDescriptionColumn.REQUIREMENT: job.requirement,
                JobDescriptionColumn.BENEFIT: job.benefit,
            })

        udf_job_desc_builder_func = udf(job_desc_builder_func)

        dw_df = df \
            .selectExpr('partition',
                        'offset',
                        'CAST(value AS STRING)',
                        JobDescriptionColumn.SOURCE,
                        JobDescriptionColumn.COLLECT_ID) \
            .withColumn('job_description_json', udf_job_desc_builder_func('value')) \
            .withColumn('job_description', from_json('job_description_json', self.job_description_schema)) \
            .withColumn(JobDescriptionColumn.ID, col('partition') * (10 ** 9) + col('offset'))

        dw_df = dw_df.select(JobDescriptionColumn.ID,
                             JobDescriptionColumn.SOURCE,
                             JobDescriptionColumn.COLLECT_ID,
                             'job_description.*')
        self.tag_stat_df = self.transform_to_tag_stat_dataframe(dw_df)
        self.spark_impl.writestream_dataframe_to_console(self.tag_stat_df,
                                                         output_count_stream=False, output_mode='complete')
        return dw_df

    def load(self, df):
        self.spark_impl.writestream_dataframe_to_elasticsearch(df,
                                                               index=self.es_index,
                                                               nodes=self.es_nodes,
                                                               es_mapping_id=JobDescriptionColumn.ID,
                                                               checkpoint=self.es_checkpoint)
        self.spark_impl.writestream_dataframe_to_medallion_table(df, self.silver_table)
        self.spark_impl.writestream_dataframe_to_medallion_table(self.tag_stat_df,
                                                                 self.gold_tags_table,
                                                                 output_mode='complete')

        return df
