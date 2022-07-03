from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator

url = 'https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls'
database = "public"

def inseret_data(url, sheet_name, schema, table):
    from func.xls_to_pg import ExcelToPG
    return ExcelToPG.get_data(url, sheet_name, schema, table)

with DAG(
    dag_id="Raize_ingestion",
    schedule_interval="@daily",
    catchup=False,
    start_date=datetime(2022, 7, 1)
) as dag:

    start = DummyOperator(
        task_id="Start"
    )
    end = DummyOperator(
        task_id="End"
    )
    # sensor_task = FileSensor( 
    #     task_id= "check_file", 
    #     poke_interval= 30, 
    #     filepath= "/opt/airflow/dag/files/vendas-combustiveis-m3.xls" 
    # )

    insert_oil_data = PythonOperator(
        task_id = "insert_oil_data",
        python_callable=inseret_data(url, "DPCache_m3", database, "raizen_oil_derivative")
    )

    insert_diesel_data = PythonOperator(
        task_id = "insert_disel_data",
        python_callable=inseret_data(url, "DPCache_m3_2", database, "raizen_disel")
    )   

    start >> insert_diesel_data >> insert_oil_data >> end