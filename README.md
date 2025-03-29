# ETL Pipeline Using Apache Airflow

I created **sales_etl_dag.py** and performed the basic steps. DAG defines were made and
here it is set to run every day during the night with `schedule_interval='0 0 * * *'`. However, it
can be triggered manually. Additionally, with `catchup=False` it does not try to catch the old
dates.

Tasks in the DAG work as follows: after **extract_postgres_task** and **extract_csv_task** start,
**transform_task** starts and then **load_task** runs. This ensures that data is processed in the correct
order.

- **extract_postgres()**:  It connects to a PostgreSQL database, retrieves sales data for a specific
date, and saves it as a CSV file.
- **extract_csv()**: It reads the previously saved CSV file and stores the data in pandas
DataFrame format.
- **transform_data()**: It cleans and transforms the data to make it analyzable. Operations such
as filling in missing data and correcting data types are performed here.
- **load_to_warehouse()**: It loads the processed data into the target data warehouse and makes
it ready for analysis.

These functions are defined as tasks inside the DAG with Airflow's **PythonOperator** structure.

I first ran the DAG with Airflow. Then I ran it manually to get the date I wanted directly. I used
the following command in the terminal:
```
airflow dags trigger -e 2024-03-04 sales_etl_dag
```

