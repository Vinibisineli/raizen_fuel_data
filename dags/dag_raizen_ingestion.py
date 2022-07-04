from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.sensors.http import HttpSensor 
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

url = 'https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls'
database = "public"

def inseret_data(url, sheet_name, schema, table):
    from func.xls_to_pg import ExcelToPG
    return ExcelToPG.get_data_from_url(url, sheet_name, schema, table)

with DAG(
    dag_id="Raize_ingestion",
    schedule_interval="@daily",
    catchup=False,
    template_searchpath='/opt/airflow/plugins/sql',
    start_date=datetime(2022, 7, 1)
) as dag:

    start = DummyOperator(
        task_id="Start"
    )
    end = DummyOperator(
        task_id="End"
    )
    
    sensor_task = HttpSensor( 
         task_id= "check_url", 
         poke_interval= 30, 
         http_conn_id= "URL_CONNID",
         endpoint = "/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls",
         method = 'GET'
     )

    insert_oil_data = PythonOperator(
        task_id = "insert_oil_data",
        python_callable=inseret_data,
        op_kwargs={
            'url': url,
            'sheet_name': "DPCache_m3",
            'schema': database,
            'table': "raizen_oil_derivative"
        }
    )

    insert_diesel_data = PythonOperator(
        task_id = "insert_disel_data",
        python_callable=inseret_data,
                op_kwargs={
            'url': url,
            'sheet_name': "DPCache_m3_2",
            'schema': database,
            'table': "raizen_disel"
        }
    ) 

    create_table = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="POSTGRES_CONNID",
        sql="create_tables.sql"
    )  
    insert_oil_trsuted = PostgresOperator(
        task_id="insert_oil_agg_data",
        postgres_conn_id="POSTGRES_CONNID",
        sql="insert_oil_trusted_data.sql"
    )  
    insert_diesel_trsuted = PostgresOperator(
        task_id="insert_diesel_agg_data",
        postgres_conn_id="POSTGRES_CONNID",
        sql="insert_diesel_trusted_data.sql"
    )  


    start >> sensor_task >> [insert_diesel_data, insert_oil_data] >> create_table
    create_table >> [insert_oil_trsuted,insert_diesel_trsuted] >> end