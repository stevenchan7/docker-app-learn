from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args={
    'owner': 'steven',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

# def greet(name, age):
#     print(f"Hello I am {name} and I am {age} years old")

# Greet function with name from xcom 
def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')

    print(f"Hello I am {first_name} {last_name} and I am {age} years old")

# def get_name() -> str:
#     return "Steven Belva"

# Push key value to xcom
def get_name(ti):
    ti.xcom_push(key='first_name', value='steven')
    ti.xcom_push(key='last_name', value='belva')

def get_age(ti):
    ti.xcom_push(key='age', value=20)


with DAG(
    dag_id='task_with_python_operator_v3',
    default_args=default_args,
    description='dag with python operator',
    start_date=datetime(2024, 6, 20),
    schedule_interval='@daily'
) as dag:
    t1=PythonOperator(
        task_id='hello_world',
        python_callable=greet,
        # op_kwargs={ 'name': 'steven', 'age': 30 }
    )

    t2=PythonOperator(
        task_id="get_name",
        python_callable=get_name
    )

    t3=PythonOperator(
        task_id="get_age",
        python_callable=get_age
    )

    [t2, t3] >> t1