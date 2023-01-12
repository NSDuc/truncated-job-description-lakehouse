import os.path

import dotenv

ELASTICSEARCH_INDEX_0 = "ELASTICSEARCH_INDEX_SILVER"
ELASTICSEARCH_INDEX_1 = "ELASTICSEARCH_INDEX_GOLD"
ELASTICSEARCH_HOSTS = "ELASTICSEARCH_HOSTS"
KAFKA_BOOTSTRAP_SERVERS = "KAFKA_BOOTSTRAP_SERVERS"
KAFKA_TOPIC = "KAFKA_TOPIC"
SPARK_STORAGE_PATH = "SPARK_STORAGE_PATH"
SPARK_PROCESS_PATH = "SPARK_PROCESS_PATH"
SPARK_ANALYSIS_PATH = "SPARK_ANALYSIS_PATH"
SPARK_ELASTICSEARCH_CHECKPOINT = "SPARK_ELASTICSEARCH_CHECKPOINT"
DELTA_WORD_CLOUD_PATH = "DELTA_WORD_CLOUD_PATH"
ANALYZED_WORD_CLOUD_PATH_0 = "ANALYZED_WORD_CLOUD_PATH_0"
ANALYZED_WORD_CLOUD_PATH_1 = "ANALYZED_WORD_CLOUD_PATH_1"
TAG_FILE_RELATIVE_PATH = "TAG_FILE_RELATIVE_PATH"


class TestConfigLoader:
    @staticmethod
    def load_config(dotenv_filename=".env.test"):
        path = dotenv.find_dotenv(dotenv_filename)
        return dotenv.dotenv_values(dotenv_path=path)


class TestContext:
    def __init__(self):
        config = TestConfigLoader.load_config()
        self.kafka_bootstrap_servers = config[KAFKA_BOOTSTRAP_SERVERS]
        self.kafka_topic = config[KAFKA_TOPIC]

        self.elasticsearch_index_0 = config[ELASTICSEARCH_INDEX_0]
        self.elasticsearch_index_1 = config[ELASTICSEARCH_INDEX_1]
        self.elasticsearch_hosts = config[ELASTICSEARCH_HOSTS]

        self.storage_path = config[SPARK_STORAGE_PATH]
        self.process_path = config[SPARK_PROCESS_PATH]
        self.analysis_path = config[SPARK_ANALYSIS_PATH]
        self.elasticsearch_checkpoint = config[SPARK_ELASTICSEARCH_CHECKPOINT]

        self.delta_word_cloud_path = config[DELTA_WORD_CLOUD_PATH]
        self.analyzed_word_cloud_path_0 = config[ANALYZED_WORD_CLOUD_PATH_0]
        self.analyzed_word_cloud_path_1 = config[ANALYZED_WORD_CLOUD_PATH_1]

        self.tag_file_path = os.path.join("..", config[TAG_FILE_RELATIVE_PATH])
