from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args={
    'owner': 'steven',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def greet(name, age):
    print(f"Hello I am {name} and I am {age} years old")

with DAG(
    dag_id='task_with_python_operator_v1',
    default_args=default_args,
    description='dag with python operator',
    start_date=datetime(2024, 6, 20),
    schedule_interval='@daily'
) as dag:
    t1=PythonOperator(
        task_id='hello world',
        python_callable=greet,
        op_kwargs={ 'name': 'steven', 'age': 30 }
    )

    t1