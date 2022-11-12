from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

"""
from includes import extract
from includes import transform
from includes import load"""


def _processExtract():
    print("Prueba Extracto EXITOSO!")

def _processTransform():
    print("Prueba Transform EXITOSO!")

def _processLoad():
    print("Prueba Load EXITOSO!")

with DAG(
    dag_id='ETL',
    start_date=datetime(2022, 11, 11),
    #schedule_interval=None
    schedule_interval='@daily',
    catchup=False
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )

    t1 = PythonOperator(
        task_id='Extract',
        python_callable=_processExtract
        #python_callable=extract
    )

    t2 = PythonOperator(
        task_id='Transform',
        python_callable=_processTransform
    )

    t3 = PythonOperator(
        task_id='Load',
        python_callable=_processLoad
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> t1
t1>> t2
t2 >> t3
t3 >> end_task