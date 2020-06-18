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
        storage_bucket='listery-staging',
        num_workers=2,
        master_disk_size=20,
        worker_disk_size=20,
        num_preemptible_workers=1,
        zone='us-east1-c',
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1')

    run_integration_job = dataproc_operator.DataProcSparkOperator(
        task_id='run_integration_job',
        main_jar=SPARK_JOBS_JAR,
        cluster_name='composer-data-integration-cluster-{{ ds_nodash }}',
        arguments=["--local", "false", "--subprogram", "integration"])

    offer_integration = dataproc_operator.DataProcSparkOperator(
        task_id='offer_integration',
        main_jar=SPARK_JOBS_JAR,
        cluster_name='composer-data-integration-cluster-{{ ds_nodash }}',
        arguments=["--local", "false", "--subprogram", "offerIntegration"])

    es_refresh = dataproc_operator.DataProcSparkOperator(
        task_id='es_refresh',
        main_jar=SPARK_JOBS_JAR,
        cluster_name='composer-data-integration-cluster-{{ ds_nodash }}',
        arguments=["--local", "false", "--subprogram", "refreshEs"])

    price_diff = dataproc_operator.DataProcSparkOperator(
        task_id='price_diff',
        main_jar=SPARK_JOBS_JAR,
        cluster_name='composer-data-integration-cluster-{{ ds_nodash }}',
        arguments=["--local", "false", "--subprogram", "priceDiff"])

    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='composer-data-integration-cluster-{{ ds_nodash }}',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    create_dataproc_cluster >> [run_integration_job, offer_integration]
    run_integration_job >> price_diff
    [run_integration_job, offer_integration] >> es_refresh
    [price_diff, es_refresh] >> delete_dataproc_cluster
