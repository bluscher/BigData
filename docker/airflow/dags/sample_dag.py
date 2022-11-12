from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

with DAG(
    dag_id='first_sample_dag',
    start_date=datetime(2022, 5, 28),
    schedule_interval=None
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )

    t1 = BashOperator(
        task_id='Extract',
        bash_command='echo "Extract CSV"'
    )

    t2 = BashOperator(
        task_id='Transform',
        bash_command='echo "Transform Data"'
    )

    t3 = BashOperator(
        task_id='Load',
        bash_command='echo "Load Data"'
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> t1
t1>> t2
t2 >> t3
t3 >> end_task