import datetime

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

SPARK_JOBS_JAR = (
    'gs://listery-datalake/bdm-1.0-SNAPSHOT-all.jar')

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {

    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': 'upc-bdm'
}

with models.DAG(
        'store_data_integration',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        cluster_name='composer-data-integration-cluster-{{ ds_nodash }}',
        num_workers=2,
        zone='us-east1-c',
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1')

    run_integration_job = dataproc_operator.DataProcSparkOperator(
        task_id='run_integration_job',
        main_jar=SPARK_JOBS_JAR,
        cluster_name='composer-data-integration-cluster-{{ ds_nodash }}',
        arguments=["integration"])

    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='composer-data-integration-cluster-{{ ds_nodash }}',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    create_dataproc_cluster >> run_integration_job >> delete_dataproc_cluster
