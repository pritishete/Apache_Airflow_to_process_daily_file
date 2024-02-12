from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,  # Set catchup to False
}

dag = DAG(
    'gcp_dataproc_spark_job',
    default_args=default_args,
    description='A DAG to run Spark job on Dataproc',
    schedule_interval=timedelta(days=1),  # Schedule the DAG to run once a day
    start_date=days_ago(1),
    tags=['example'],
)



# Add GCSObjectExistenceSensor task
file_sensor_task = GCSObjectExistenceSensor(
    task_id='file_sensor_task',
    bucket='airflow-first-bucket',  # Replace with your GCS bucket name
    object='input_files/employee.csv',  # Replace with your daily CSV file path
    poke_interval=300,  # Poke every 10 seconds
    timeout=43200,  # Maximum poke duration of 12 hours
    mode='poke',
    dag=dag,
)

#Fetch configuration from Airflow variables
config = Variable.get("cluster_details", deserialize_json=True)
CLUSTER_NAME = config['CLUSTER_NAME']
PROJECT_ID = config['PROJECT_ID']
REGION = config['REGION']
#pyspark_job_file_path = 'gs://my_first_bucket111/orders_data_process.py'

# Check if execution_date is provided manually, otherwise use the default execution date
# date_variable = "{{ ds_nodash }}"
date_variable = "{{ dag_run.conf['execution_date'] if dag_run and dag_run.conf and 'execution_date' in dag_run.conf else ds_nodash }}"


pyspark_job = {
    'main_python_file_uri': 'gs://airflow-assignment-1/employee_batch_assignment_1.py'
}

submit_pyspark_job = DataprocSubmitPySparkJobOperator(
    task_id='submit_pyspark_job',
    main=pyspark_job['main_python_file_uri'],
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)



# Set task dependencies
file_sensor_task>>submit_pyspark_job