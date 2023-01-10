from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from job_desc_lakehouse.DTO.medalion_table_dto import MedallionTableDTO


class SparkServiceImpl:
    class Builder:
        @staticmethod
        def create_spark_impl_for_delta(app_name, sql_shuffle_partitions=1):
            spark_conf = {
                "spark.sql.shuffle.partitions": sql_shuffle_partitions,
                "spark.jars.packages": "io.delta:delta-core_2.12:1.2.1",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            }
            return SparkServiceImpl(app_name, spark_conf)

        @staticmethod
        def create_spark_impl_for_delta_kafka(app_name, sql_shuffle_partitions=1):
            spark_conf = {
                "spark.sql.shuffle.partitions": sql_shuffle_partitions,
                "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,"
                                       "io.delta:delta-core_2.12:1.2.1",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            }
            return SparkServiceImpl(app_name, spark_conf)

        @staticmethod
        def create_spark_impl_for_delta_elasticsearch(app_name, sql_shuffle_partitions=1):
            spark_conf = {
                "spark.sql.shuffle.partitions": sql_shuffle_partitions,
                "spark.jars.packages": "io.delta:delta-core_2.12:1.2.1",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.jars": "/workspace/es_hadoop_jars/elasticsearch-spark-30_2.12-7.17.3.jar",
            }
            return SparkServiceImpl(app_name, config=spark_conf)


    def __init__(self, app_name, config: dict):
        self.app_name = app_name
        self.config = config

        self.spark = self.get_or_create_spark_session()

    def get_or_create_spark_session(self) -> SparkSession:
        builder: SparkSession.Builder = SparkSession.builder.appName(self.app_name)
        for k, v in self.config.items():
            builder.config(k, v)
        # not used function 'configure_spark_with_delta_pip' because it overrides KEY "spark.jars.packages"
        return builder.getOrCreate()

    def readstream_dataframe_from_kafka(self, bootstrap_servers, topic) -> DataFrame:
        df = self.spark.readStream.format('kafka') \
            .option('kafka.bootstrap.servers', bootstrap_servers) \
            .option('subscribe', topic) \
            .option('startingOffsets', 'earliest') \
            .option("includeHeaders", True) \
            .load()
        return df

    def readstream_dataframe_from_medallion_table(self, table: MedallionTableDTO) -> DataFrame:
        if table.file_format() == 'delta':
            df = self.spark.readStream.format(table.file_format()) \
                .load(table.data_dirpath())
        else:
            raise NotImplementedError
        return df

    def read_dataframe_from_files(self, data_dirpath, file_format):
        if file_format == 'delta':
            df = self.spark.read.format(file_format) \
                .load(data_dirpath)
        else:
            raise NotImplementedError
        return df

    def read_dataframe_from_medallion_table(self, table: MedallionTableDTO) -> DataFrame:
        return self.read_dataframe_from_files(data_dirpath=table.data_dirpath(),
                                              file_format=table.file_format())

    def writestream_dataframe_to_medallion_table(self, df: DataFrame, table: MedallionTableDTO,
                                                 output_mode='append'):
        print(f'{self.__class__.__name__} write to table {table.base_path()}')
        writeStream = df.writeStream \
            .format(table.file_format()) \
            .outputMode(output_mode) \
            .option('path', table.data_dirpath()) \
            .option('checkpointLocation', table.checkpoint_dirpath()) \
            .trigger(once=True)
        writeStream.start().awaitTermination()

    def writestream_dataframe_to_console(self, df: DataFrame, df_describe='',
                                         output_count_stream=True,
                                         output_mode='append'):
        print(f'{self.__class__.__name__} console: {df_describe}')
        writeStream = df.writeStream \
            .format('console') \
            .outputMode(output_mode) \
            .trigger(once=True)
        writeStream.start().awaitTermination()

        if output_count_stream:
            countWriteStream = df.groupby().count().writeStream \
                .format('console') \
                .outputMode("complete") \
                .trigger(once=True)
            countWriteStream.start().awaitTermination()

    def write_dataframe_to_console(self, df: DataFrame, truncate=True):
        print(f'{self.__class__.__name__} write to table console')
        count_df = df.groupby().count()
        df.show(truncate=truncate)
        count_df.show()

    def writestream_dataframe_to_elasticsearch(self, df: DataFrame, index, nodes, es_mapping_id=None,
                                               checkpoint='/tmp/es'):
        print(f'write to elasticsearch')
        writeStream = df.writeStream.format('org.elasticsearch.spark.sql') \
            .option('checkpointLocation', checkpoint) \
            .option('es.resource', f'{index}/_doc') \
            .option('es.nodes', nodes) \
            .outputMode('append')\
            .trigger(once=True)
        if es_mapping_id:
            writeStream.option('es.mapping.id', es_mapping_id)

        writeStream.start().awaitTermination()

    def show_medallion_table_info(self, table: MedallionTableDTO = None, path=None, fmt="delta"):
        assert (table or (path and fmt))
        if not table:
            table = MedallionTableDTO(path, fmt)

        df = self.read_dataframe_from_medallion_table(table)
        print(f'number of partitions: {df.rdd.getNumPartitions()}')
        self.write_dataframe_to_console(df)
