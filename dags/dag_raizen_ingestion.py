import requests
from airflow import DAG
from datetime import datetime, timedelta
from tokenize import String
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.sensors.http import HttpSensor 
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from xlsx_provider.operators.from_xlsx_operator import FromXLSXOperator
from configparser import ConfigParser

parser = ConfigParser()
parser.read('dags/keyvault.cfg')

url = parser['raizen']['url']
endpoint = parser['raizen']['endpoint']

def run_and_download_file(url):
    resp = requests.get(url)
    with open('/opt/airflow/dags/files/raizen_report.xls', 'wb+') as f:
        f.write(resp.content)

def inseret_data(url, sheet_name, schema, table):
    from func.xls_to_pg import ExcelToPG
    return ExcelToPG.get_data_from_url(url, sheet_name, schema, table)

def insert_data(json_file: String, sql_file: String, table: String):
    from func.create_insert_pg_data import CreateInsertData
    return CreateInsertData.create_insert_data(json_file, sql_file, table)

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
        endpoint = endpoint,
        method = 'GET'
     )
    
    downalod_file = PythonOperator(
        task_id = "downalod_file",
        python_callable=run_and_download_file,
        op_kwargs={
            'url': url
        }
    ) 

    xlsx_to_jsonl_oil = FromXLSXOperator(
        task_id='xlsx_to_json_oil',
        source='/opt/airflow/dags/files/vendas-combustiveis-m3.xls',
        target='/opt/airflow/dags/files/vendas-combustiveis-m3-oil.json',
        file_format='json',
        worksheet=1
    )

    xlsx_to_jsonl_diesel = FromXLSXOperator(
        task_id='xlsx_to_json_diesel',
        source='/opt/airflow/dags/files/vendas-combustiveis-m3.xls',
        target='/opt/airflow/dags/files/vendas-combustiveis-m3-diesel.json',
        file_format='json',
        worksheet=1
    )

    create_insert_oil_data = PythonOperator(
        task_id = "create_insert_oil_data",
        python_callable=insert_data,
        op_kwargs={
            'json_file': '/opt/airflow/dags/files/vendas-combustiveis-m3-oil.json',
            'sql_file': 'raw_oil_data',
            'table': "raizen_oil_derivative"
        }
    )

    create_insert_diesel_data = PythonOperator(
        task_id = "create_insert_diesel_data",
        python_callable=insert_data,
        op_kwargs={
            'json_file': '/opt/airflow/dags/files/vendas-combustiveis-m3-diesel.json',
            'sql_file': 'raw_diesel_data',
            'table': "raizen_diesel"
        }
    ) 

    create_table = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="POSTGRES_CONNID",
        sql="create_tables.sql"
    ) 
    insert_oil_raw = PostgresOperator(
        task_id="insert_oil_raw_data",
        postgres_conn_id="POSTGRES_CONNID",
        sql="raw_oil_data.sql"
    )   
    insert_oil_trusted = PostgresOperator(
        task_id="insert_oil_trusted_data",
        postgres_conn_id="POSTGRES_CONNID",
        sql="insert_oil_trusted_data.sql"
    )  

    insert_diesel_raw = PostgresOperator(
        task_id="insert_diesel_raw_data",
        postgres_conn_id="POSTGRES_CONNID",
        sql="raw_diesel_data.sql"
    )   
    insert_diesel_trusted = PostgresOperator(
        task_id="insert_diesel_trusted_data",
        postgres_conn_id="POSTGRES_CONNID",
        sql="insert_diesel_trusted_data.sql"
    )  


    #start >> sensor_task >> [insert_diesel_data, insert_oil_data] >> create_table
    #create_table >> [insert_oil_trsuted,insert_diesel_trsuted] >> end
    start >> sensor_task >> downalod_file >> create_table >> [xlsx_to_jsonl_oil,xlsx_to_jsonl_diesel] 
    xlsx_to_jsonl_oil >> create_insert_oil_data >> insert_oil_raw >> insert_oil_trusted >> end
    xlsx_to_jsonl_diesel >> create_insert_diesel_data >> insert_diesel_raw >> insert_diesel_trusted >> end