from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),  
    'retries': 1,
}

dag = DAG(
    'sales_etl_dag',
    default_args=default_args,
    schedule_interval='0 0 * * *',  
    catchup=False,
    description='ETL pipeline for sales data'
)

def extract_postgres(**kwargs):
    ds = kwargs['ds']
    conn = psycopg2.connect(
        dbname='sales_db',
        user='aleynakurt',
        password='9701',
        host='host.docker.internal',
        port='5432'  
    )
    query = f"""
    SELECT product_id, quantity, sale_amount
    FROM online_sales
    WHERE sale_date = '{ds}';
    """
    df = pd.read_sql(query, conn)
    conn.close()
    df.to_csv('/opt/airflow/data/postgres_extract.csv', index=False)

extract_postgres_task = PythonOperator(
    task_id='extract_postgres',
    python_callable=extract_postgres,
    provide_context=True,  
    dag=dag
)

def extract_csv(**kwargs):
    ds = kwargs['ds']
    csv_path = '/opt/airflow/data/in_store_sales.csv'
    df = pd.read_csv(csv_path)
    df = df[df['sale_date'] == ds]
    df = df[['product_id', 'quantity', 'sale_amount']]
    df.to_csv('/opt/airflow/data/temp_csv.csv', index=False)

extract_csv_task = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_csv,
    provide_context=True,
    dag=dag
)

def transform_data():
    pg_data = pd.read_csv('/opt/airflow/data/postgres_extract.csv')
    csv_data = pd.read_csv('/opt/airflow/data/temp_csv.csv')
    combined = pd.concat([pg_data, csv_data])
    combined = combined.dropna()  
    aggregated = combined.groupby('product_id').agg({
        'quantity': 'sum',
        'sale_amount': 'sum'
    }).reset_index()
    aggregated.columns = ['product_id', 'total_quantity', 'total_sale_amount']
    aggregated.to_csv('/opt/airflow/data/transformed_data.csv', index=False)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

def load_to_warehouse():
    conn = psycopg2.connect(
        dbname='warehouse_db',
        user='aleynakurt',
        password='9701',
        host='host.docker.internal',
        port='5432'
    )
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sales_summary (
        product_id INT PRIMARY KEY,  -- product_id'yi birincil anahtar yap
        total_quantity INT,
        total_sale_amount DECIMAL(10, 2)
    );
    """)
    df = pd.read_csv('/opt/airflow/data/transformed_data.csv')
    for _, row in df.iterrows():
        cursor.execute("""
        INSERT INTO sales_summary (product_id, total_quantity, total_sale_amount)
        VALUES (%s, %s, %s)
        ON CONFLICT (product_id) DO UPDATE SET
            total_quantity = EXCLUDED.total_quantity,
            total_sale_amount = EXCLUDED.total_sale_amount;
        """, (row['product_id'], row['total_quantity'], row['total_sale_amount']))
    conn.commit()
    cursor.close()
    conn.close()

load_task = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    dag=dag
)

extract_postgres_task >> transform_task
extract_csv_task >> transform_task
transform_task >> load_task