from pyspark.sql import SparkSession

def process_data():
    print("Inside process_Data")
    spark = SparkSession.builder.appName("GCPDataprocJob").config("spark.sql.warehouse.dir","/airflow-assignment-1/hive_data").enableHiveSupport().getOrCreate()
    """When working with Hive, one must instantiate SparkSession with Hive support, 
    including connectivity to a persistent Hive metastore, support for Hive serdes, 
    and Hive user-defined functions. Users who do not have an existing Hive deployment can still enable Hive support. 
    When not configured by the hive-site.xml, the context automatically creates metastore_db in the current directory 
    and creates a directory configured by spark.sql.warehouse.dir, 
    which defaults to the directory spark-warehouse in the current directory that the Spark application is started.
     Note that the hive.metastore.warehouse.dir property in hive-site.xml is deprecated since Spark 2.0.0.
      Instead, use spark.sql.warehouse.dir to specify the default location of database in warehouse. You may need to grant write privilege to the user who starts the Spark application.
    """

    print("Spark session has created successfully")

    # Define your GCS bucket and paths
   
    emp_data_path = f"gs://airflow-first-bucket/input_files/employee.csv"

    # Read datasets
    employee = spark.read.csv(emp_data_path, header=True, inferSchema=True)


    # Filter employee data
    filtered_employee = employee.filter(employee.salary >=60000) # Adjust the salary threshold as needed
    
    # Hive database and table names
    # hive_database_name = "airflow"
    # hive_table_name = "filtered_employee"

    # HQL to create the Hive database if it doesn't exist
    hive_create_database_query = f"CREATE DATABASE IF NOT EXISTS airflow"
    spark.sql(hive_create_database_query)

# HQL to create the Hive table inside the 'airflow' database if it doesn't exist
    hive_create_table_query = f"""
        CREATE TABLE IF NOT EXISTS airflow.filtered_employee (
        emp_id INT,
        emp_name STRING,
        dept_id INT,
        salary INT
        )
        STORED AS PARQUET
    """
    # Execute the HQL to create the Hive table
    spark.sql(hive_create_table_query)

    # # Write the filtered employee data to the Hive table in append mode
    filtered_employee.write.mode("append").format("hive").saveAsTable("airflow.filtered_employee")

    

if __name__ == "__main__":
    process_data()