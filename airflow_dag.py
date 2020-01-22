import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from datetime import timedelta, datetime

#py script athena.py supposed to be saved in airflow/dags/jobs/amplitude_feed directory
from jobs.amplitude_feed.athena import run_add_partitions

args = {
    'owner': 'perfect_company',
    'depends_on_past': False,
    'start_date': datetime(2020, 01, 22, 6, 0, 0),
    'email': ['data-monitor@perfect_company.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

schedule = "15 0 * * *"

dag = DAG(
    dag_id="etl_amplitude_feed_daily",
    default_args=args,
    schedule_interval=schedule,
    max_active_runs=1,
    catchup=True,
)

#assume we have Databricks integration an repartition.py is saved in Databricks catalog

#create cluster in Databricks with 1 worker and attach iam role which has access to s3 bucked with data
etl_cluster = {
    'spark_version': '4.0.x-scala2.11',
    'node_type_id': 'i3.xlarge',
    'aws_attributes': {
        'availability': 'ON_DEMAND',
        'instance_profile_arn': 'arn:aws:iam::00000000000:instance-profile/de-instance-profile'
    },
    'num_workers': 1
}

#set path to repartition.py file in Databricks catalog
notebook_params = {
    'new_cluster': etl_cluster,
    'notebook_task': {'notebook_path': '/path_to_file_in_databricks/repartition'}
}

run_process_data = DatabricksSubmitRunOperator(
    task_id='process_data',
    json=notebook_params,
    retries=2,
    dag=dag)


### V1 with PythonOperator which executes run_add_partitions func from athena.py 
run_repair_partition = PythonOperator(
    task_id="repair_partition",
    dag=dag,
    python_callable=run_add_partitions,
    execution_timeout=timedelta(minutes=10),
    provide_context=True,
)

### V2 with AWSAthenaOperator
run_repair_partition = AWSAthenaOperator(
        task_id='repair_partition',
        query='MSCK REPAIR TABLE amplitude_feed',
        output_location='s3://my-bucket/my-path/',
        database='my_database'
    )

(
    run_process_data >>
    run_repair_partition
)
