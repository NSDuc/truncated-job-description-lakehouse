from pprint import pprint

import pendulum
from os.path import join
from pyspark.sql import DataFrame
from job_desc_lakehouse.services.wordcloud_service import WordCloudServiceImpl
from job_desc_lakehouse.services.etl_service import ETLService
from job_desc_lakehouse.services.spark_service import SparkServiceImpl
from job_desc_lakehouse.DTO.medalion_table_dto import MedallionTableDTO


class AnalysisStageServiceImpl(ETLService):
    def __init__(self, spark_iml: SparkServiceImpl, **kwargs):
        self.spark_impl: SparkServiceImpl = spark_iml
        analysis_path = kwargs['analysis_path']

        self.silver_table = MedallionTableDTO(base_path=kwargs['process_path'])
        self.tag_count_table = MedallionTableDTO(join(analysis_path, 'tags'))
        self.tags_df = None

        self.word_cloud_path = kwargs['cloud_word_path']

    def exact(self):
        self.tags_df = self.spark_impl.read_dataframe_from_medallion_table(self.tag_count_table)
        return self.tags_df

    def transform(self, tag_stat_df: DataFrame):
        tag_stat_rows = tag_stat_df.collect()
        tag_freq = {}
        for row in tag_stat_rows:
            tag_freq[row[0]] = int(row[1])

        pprint(tag_freq)
        return tag_freq

    def load(self, tag_freq):
        WordCloudServiceImpl.generate_word_cloud(frequency=tag_freq, savefig_path=self.word_cloud_path)
