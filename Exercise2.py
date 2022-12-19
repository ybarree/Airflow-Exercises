import time
from datetime import datetime, timedelta 
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator

def time_function(): 
    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Current datetime: {current_datetime}")
    return current_datetime

default_dag_args = { 'start_date': datetime(2022, 12, 15), 'email_on_failure': False, 
'email_on_retry': False, 'retries': 1, 'retry_delay': timedelta(minutes=5), 'project_id': 1 }

with DAG("current_time_dag", schedule_interval = '@daily', catchup=False, default_args = default_dag_args) as dag_python:
    task_0 = PythonOperator(task_id = "python_task", python_callable = python_first_function)
