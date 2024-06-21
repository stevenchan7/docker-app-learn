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

    t2 = BashOperator(
        task_id='second_task',
        bash_command="echo 'this is task 2 which will be executed after task 1'"
    )

    t3 = BashOperator(
        task_id='third task',
        bash_command="echo 'this is task 3, executed after task 1 and simultaneously with task 2'"
    )

    # Option 1
    t1.set_downstream(t2)
    t1.set_downstream(t3)

    # Option 2
    t1 >> t2
    t1 >> t3

    # Option 3
    t1 >> [t2, t3]