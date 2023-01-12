import pendulum
from pendulum import DateTime
from typing import Optional
from airflow.hooks.base import BaseHook
from airflow.models import DAG, Variable
from airflow.decorators import task
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import ShortCircuitOperator
from job_desc_lakehouse.services.kafka_service import KafkaProducerImpl
from job_desc_lakehouse.services.collect_service import JDCollectOptions, JDCollectServiceImpl
from job_desc_lakehouse.services.spark_service import SparkServiceImpl
from job_desc_lakehouse.services.storage_stage_service import StorageStageServiceImpl
from job_desc_lakehouse.services.processing_stage_service import ProcessingStageServiceImpl
from job_desc_lakehouse.services.analysis_stage_service import AnalysisStageServiceImpl


class AirflowService:
    def __init__(self, service_name, service_conn_id, service_cmd_start, service_cmd_stop, managed_by_airflow=True):
        self.service_name = service_name
        self.service_conn_id = service_conn_id
        self.service_hook = BaseHook.get_connection(service_conn_id)
        # Airflow pitfall: Airflow tries to apply a Jinja template
        self.service_cmd_start = service_cmd_start + ' '
        self.service_cmd_stop  = service_cmd_stop + ' '
        # Airflow Timeout for slow service(s)
        self.service_cmd_timeout = 120

        # Airflow create task to start/stop services
        self.managed_by_airflow = managed_by_airflow

    def get_conn_extra_dejson(self):
        return self.service_hook.extra_dejson.copy()

    def get_conn_extra_field(self, field_name):
        return self.service_hook.extra_dejson.get(field_name)

    def ssh_operator(self, command, task_id) -> Optional[SSHOperator]:
        if self.managed_by_airflow:
            return SSHOperator(ssh_conn_id=self.service_conn_id,
                               command=command, task_id=task_id,
                               cmd_timeout=self.service_cmd_timeout)
        else:
            return None

    def start_operator(self) -> Optional[SSHOperator]:
        return self.ssh_operator(command=self.service_cmd_start,
                                 task_id=f'{self.service_name}_start')

    def stop_operator(self) -> Optional[SSHOperator]:
        return self.ssh_operator(command=self.service_cmd_stop,
                                 task_id=f'{self.service_name}_stop')


kafka = AirflowService(service_name='kafka', service_conn_id='job_desc_kafka',
                       service_cmd_start='cd /opt/kafka_2.13-3.1.1/ && ./autostart.sh',
                       service_cmd_stop='cd /opt/kafka_2.13-3.1.1/ && ./autostop.sh',
                       managed_by_airflow=False)

hadoop = AirflowService(service_name='hadoop', service_conn_id='job_desc_hadoop',
                        service_cmd_start='cd /opt/hadoop-3.3.0/sbin/ && ./start-all.sh',
                        service_cmd_stop='cd /opt/hadoop-3.3.0/sbin/ && ./stop-all.sh',
                        managed_by_airflow=False)

elasticsearch = AirflowService(service_name='elasticsearch', service_conn_id='job_desc_elasticsearch',
                               service_cmd_start='systemctl restart elasticsearch',
                               service_cmd_stop='systemctl stop elasticsearch',
                               managed_by_airflow=False)

default_args = {
    'depends_on_past': True,
    'email': ['nsduc97@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(seconds=30),
}


with DAG(dag_id='job_description_pipeline',
         description='pull job descriptions from source(s) then storage, process and analyze',
         start_date=pendulum.datetime(2022, 11, 1, tz="UTC"),
         default_args=default_args,
         catchup=False,
         schedule_interval=None) as dag:
    with TaskGroup(group_id='Integration_Stage') as stage_integration:
        with TaskGroup(group_id=f'Pre_Integration') as pre_run1:
            kafka.start_operator()

        @task(task_id='Integration', do_xcom_push=True)
        def integration_new_job_descriptions(ds=None, **kwargs):
            logical_date : DateTime = kwargs['logical_date']
            kafka_producer = KafkaProducerImpl(bootstrap_servers=kafka.get_conn_extra_field('bootstrap_servers'),
                                               topic=kafka.get_conn_extra_field('topic'))

            job_collect_options = JDCollectOptions(
                max_job_collect_per_task=Variable.get('JOB_DESC_MAXIMUM_COLLECTED_JOBS_PER_TASK', 1000),
                last_collect_job_id=int(Variable.get('JOB_DESC_LAST_COLLECTED_JOB_ID', 0))
            )
            job_collector = JDCollectServiceImpl()

            jobs = job_collector.collect_job_descriptions(job_collect_options)
            for i, job in enumerate(jobs):
                print(f'{i:2} {job}')
                msg_headers = [('source', job.source.encode()),
                               ('message_group_id', logical_date.to_day_datetime_string().encode())]
                msg_key = None
                msg = job.raw
                kafka_producer.send(message=msg, headers=msg_headers, key=msg_key)

            if len(jobs):
                Variable.set('JOB_DESC_LAST_COLLECTED_JOB_ID', jobs[0].id)

            return len(jobs)

        run1 = integration_new_job_descriptions()

        Label('Start kafka services') >> pre_run1 >> Label('Integrate') >> run1

    with TaskGroup(group_id='Storage_Stage') as stage_storage:
        with TaskGroup(group_id='Pre_Storage') as pre_run2:
            hadoop.start_operator()

        with TaskGroup(group_id='Post_Storage') as post_run2:
            kafka.stop_operator()

        @task(task_id='Integration', do_xcom_push=False)
        def storage_new_job_descriptions(ds=None, **kwargs):
            spark_impl = SparkServiceImpl.Builder.create_spark_impl_for_delta_kafka('storage')

            storage_impl = StorageStageServiceImpl(spark_impl=spark_impl,
                                                   kafka_bootstrap_servers=kafka.get_conn_extra_field('bootstrap_servers'),
                                                   kafka_topic=kafka.get_conn_extra_field('topic'),
                                                   storage_path=hadoop.get_conn_extra_field('storage_path'),
                                                   storage_file_fmt='delta')
            storage_impl.run_etl()

        run2 = storage_new_job_descriptions()

        Label('Start Hadoop services') >> pre_run2 >> Label('Store') >> run2 >> \
            Label('Stop Kafka services') >> post_run2

    with TaskGroup(group_id='Processing_Stage') as stage_processing:
        with TaskGroup(group_id='Pre_Processing') as pre_run3:
            elasticsearch.start_operator()


        @task(task_id='Processing', do_xcom_push=False)
        def process_new_job_descriptions(ds=None, **kwargs):
            spark_impl = SparkServiceImpl.Builder.create_spark_impl_for_delta_elasticsearch(app_name='test_processing')

            process_impl = ProcessingStageServiceImpl(spark_impl,
                                                      storage_path=hadoop.get_conn_extra_field('storage_path'),
                                                      storage_file_fmt='delta',
                                                      process_path=hadoop.get_conn_extra_field('process_path'),
                                                      process_file_fmt='delta',
                                                      analysis_path=hadoop.get_conn_extra_field('analysis_path'),
                                                      analysis_file_fmt='delta',
                                                      elasticsearch_index=elasticsearch.get_conn_extra_field('index'),
                                                      elasticsearch_nodes=elasticsearch.get_conn_extra_field('nodes'),
                                                      elasticsearch_checkpoint=hadoop.get_conn_extra_field('elasticsearch_checkpoint'))
            process_impl.run_etl()

        run3 = process_new_job_descriptions()
        Label('Start elasticsearch services') >> pre_run3 >> Label('Process') >> run3

    with TaskGroup(group_id='Analysis_Stage') as stage_analysis:
        with TaskGroup(group_id='Post_Analysis') as post_run4:
            hadoop.stop_operator()
            elasticsearch.stop_operator()


        @task(task_id='Analysis', do_xcom_push=False)
        def analyze_new_job_descriptions(ds=None, **kwargs):
            spark_impl = SparkServiceImpl.Builder.create_spark_impl_for_delta(app_name='test_analysis')
            analysis_impl = AnalysisStageServiceImpl(spark_impl,
                                                     process_path=hadoop.get_conn_extra_field('process_path'),
                                                     process_file_fmt='delta',
                                                     analysis_path=hadoop.get_conn_extra_field('analysis_path'),
                                                     analysis_file_fmt='delta',
                                                     savefig_file='/tmp/output.png')
            analysis_impl.run_etl()

        run4 = analyze_new_job_descriptions()
        Label('Analyze') >> run4 >> Label('Stop all services') >> post_run4


    def check_if_storage_stage_trigger(**kwargs):
        ti = kwargs['ti']
        job_desc_num = ti.xcom_pull(task_ids='Integration_Stage.Integration')
        return job_desc_num > 0


    whether_trigger_storage_stage = ShortCircuitOperator(
        task_id="check_if_require_storage_stage",
        python_callable=check_if_storage_stage_trigger,
    )

    stage_integration >> whether_trigger_storage_stage >> \
        stage_storage >> stage_processing >> stage_analysis
