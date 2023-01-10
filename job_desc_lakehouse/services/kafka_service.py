import json
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError


class KafkaProducerImpl:
    def __init__(self, bootstrap_servers, topic):
        self._bootstrap_servers = bootstrap_servers
        self._topic    = topic
        self._producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                       value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def send(self, topic=None, message=None, headers=None, key=None):
        topic = topic or self._topic
        return self._producer.send(topic, message, headers=headers, key=key)


class KafkaAdminClientImpl:
    def __init__(self, bootstrap_servers):
        self._bootstrap_servers = bootstrap_servers
        self._admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers, api_version=(3, 1, 1))

    def create_topic(self, topic):
        try:
            new_topic = NewTopic(name=topic, num_partitions=1, replication_factor=1)
            self._admin_client.create_topics(new_topics=[new_topic])
            print(f'Topic {topic} is created successfully')
        except TopicAlreadyExistsError as e:
            print(f'Topic {topic} is already exists: {e}')

    def delete_topic(self, topic):
        try:
            self._admin_client.delete_topics(topics=[topic])
            print(f'Topic {topic} is deleted successfully')
        except UnknownTopicOrPartitionError as e:
            print(f'Topic {topic} is not Exist: {e}')

    def get_topics(self):
        return self._admin_client.list_topics()
