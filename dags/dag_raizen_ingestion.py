from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor

with DAG(
    dag_id="Raize_ingestion",
    schedule_interval="@daily",
    catchup=False,
    start_date=datetime(2022, 7, 1)
) as dag:

    start = DummyOperator(
        task_id="Start"
    )

    # sensor_task = FileSensor( 
    #     task_id= "check_file", 
    #     poke_interval= 30, 
    #     filepath= "/opt/airflow/dag/files/vendas-combustiveis-m3.xls" 
    # )


    start >> sensor_task