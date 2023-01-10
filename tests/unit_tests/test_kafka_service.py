import json
import time
import pendulum
from test_common import TestConfigLoader, KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS
from pprint import pprint
from unittest import TestCase
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from job_desc_lakehouse.services.kafka_service import KafkaAdminClientImpl, KafkaProducerImpl


class TestKafkaAdminClientImpl(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestKafkaAdminClientImpl, self).__init__(*args, **kwargs)
        conf = TestConfigLoader.load_config()
        self.bootstrap_server = conf[KAFKA_BOOTSTRAP_SERVERS]
        self.topic = conf[KAFKA_TOPIC]
        print(self.bootstrap_server, self.topic)
        self.impl = KafkaAdminClientImpl(KAFKA_BOOTSTRAP_SERVERS)

    def test_connection(self):
        self.assertIsNotNone(self.impl)

    def test_get_topics(self):
        topics = self.impl.get_topics()
        pprint(topics)

    def test_create_topic(self):
        self.impl.create_topic(self.topic)
        topics = self.impl.get_topics()
        self.assertIn(self.topic, topics, f'Create topic {self.topic} failed')
        print(f'Exist {self.topic} in {topics}')

    def test_delete_topic(self):
        self.impl.delete_topic(self.topic)
        self.assertNotIn(self.topic, self.impl.get_topics(), f'Delete topic {self.topic} failed')


class TestKafkaProducerConsumerImpl(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestKafkaProducerConsumerImpl, self).__init__(*args, **kwargs)
        self.admin_client_impl = KafkaAdminClientImpl('localhost:9092')
        self.topic = 'tmp'
        self.producer_impl = KafkaProducerImpl('localhost:9092', self.topic)

    def test_consumer_read(self):
        consumer = KafkaConsumer(self.topic,
                                 bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 enable_auto_commit=True,
                                 value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        record: ConsumerRecord = next(consumer)
        print(f'receive topic={record.topic}, message={record.value}, offset={record.offset}')
        self.assertEqual(record.topic, self.topic)
        consumer.close()

    def test_send(self, send_times=3):
        self.admin_client_impl.create_topic(self.topic)

        consumer = KafkaConsumer(self.topic,
                                 bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 enable_auto_commit=True,
                                 value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        for i in range(send_times):
            send_message = pendulum.now().to_datetime_string()
            self.producer_impl.send('tmp', send_message)
            print(f'send topic={self.topic} message={send_message}')
            record: ConsumerRecord = next(consumer)
            print(f'receive back topic={record.topic}, message={record.value}, offset={record.offset}')
            self.assertEqual(record.value, send_message, f"expect: '{send_message}', not '{record.value}'")
            time.sleep(1)

        consumer.close()
        self.admin_client_impl.delete_topic(self.topic)
