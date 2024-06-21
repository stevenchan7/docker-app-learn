from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args={
    'owner': 'steven',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='my_first_dag',
    description='This is the first DAG that I created',
    default_args=default_args,
    start_date=datetime(2024, 6, 21, 2),
    schedule_interval='@daily'
) as dag:
    t1 = BashOperator(
        task_id='first_task',
        bash_command="echo 'hello world, this is the first task!'"
    )

    t1