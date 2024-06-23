from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args={
    'owner': 'steven belva',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id ='dag_with_taskflow_api_v2',
    default_args=default_args,
    start_date=datetime(2024, 6, 21),
    schedule_interval='@daily'
)
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'steven',
            'last_name': 'belva'
        }
    
    @task()
    def get_age():
        return 19
    
    @task()
    def greet(first_name, last_name, age):
        print(f"hello I am {first_name} {last_name} and I am {age} years old")

    name_dict=get_name()
    age=get_age()
    greet(first_name=name_dict['first_name'], last_name=name_dict['last_name'], age=age)

hello_world_etl()