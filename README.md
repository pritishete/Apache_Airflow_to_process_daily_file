
# Apache_Airflow_To_Process_Daily_File

Automate  a data pipeline using Apache Airflow to process daily incoming CSV files from a GCP bucket using a Dataproc PySpark job and save the transformed data into a Hive table.


## Tech Stack

**Language:** Python

**Cloud platform:** GCP

**Datawarehouse:** Hive

**Orchestration tool:** Apache Airflow

**Distributed Computation Framework:** PySpark






## Steps to run 
* Login to GCP account and create hadoop cluster using 
  dataproc service
* Create airflow cluster using composer service
* Create a bucket on google cloud storage
* Place employee_data_processing.py file and  
  employee.csv in the above created bucket 
  
* Once airflow cluster get created then open Airflow 
  WebUI and click on DAG and place the 
  airflow_job_schedular.py in it.As per the schedule 
  time , airflow job will trigger and look for the   
  input csv file in gcp bucket once file get found 
  it will transform the data as per the logic mentioned
  in the employee_data_processing.py and create the 
  hive table and load the transformed data into it
