from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import element_at, col
from pyspark.sql.types import StringType, IntegerType

from job_desc_lakehouse.DTO.job_desc_dto import JobDescriptionColumn
from job_desc_lakehouse.DTO.medalion_table_dto import MedallionTableDTO
from job_desc_lakehouse.services.spark_service import SparkServiceImpl
from job_desc_lakehouse.services.etl_service import ETLService


class StorageStageServiceImpl(ETLService):
    def __init__(self, spark_impl: SparkServiceImpl, **kwargs):
        super(StorageStageServiceImpl, self).__init__()
        self.spark_impl: SparkServiceImpl = spark_impl

        self.kafka_bootstrap_servers = kwargs['kafka_bootstrap_servers']
        self.kafka_topic = kwargs['kafka_topic']
        self.bronze_table = MedallionTableDTO(base_path=kwargs['storage_path'])

    def exact(self) -> DataFrame:
        return self.spark_impl.readstream_dataframe_from_kafka(self.kafka_bootstrap_servers, self.kafka_topic)

    def transform(self, df: DataFrame) -> DataFrame:
        new_df: DataFrame = df \
            .selectExpr('CAST(value AS STRING)',
                        'CAST(partition AS LONG)',
                        'CAST(offset AS LONG)',
                        'headers',
                        'timestamp',
                        'timestampType') \
            .withColumn('source_kv', element_at(col('headers'), 1)) \
            .withColumn(JobDescriptionColumn.SOURCE, col('source_kv.value').cast(StringType())) \
            .drop('source_kv') \
            .withColumn('collect_id_kv', element_at(col('headers'), 2)) \
            .withColumn(JobDescriptionColumn.COLLECT_ID, col('collect_id_kv.value').cast(StringType()).cast(IntegerType())) \
            .drop('collect_id_kv') \
            .drop('headers')
        return new_df

    def load(self, new_df: DataFrame):
        return self.spark_impl.writestream_dataframe_to_medallion_table(new_df, self.bronze_table)
