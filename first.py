from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


soruce= "/Users/yasminbarree/DATA_CENTER/DATA_LAKE/DATASET_RAW.txt"
target= "/Users/yasminbarree/DATA_CENTER/CLEAN_DATA"


default_dag_args = {
'start_date': datetime (2022, 1, 1),
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
'retry delay': timedelta (minutes=5),
'project_id': 1
}
# defining the DAG

with DAG("first_DAG" ,schedule_interval = None, default_args = default_dag_args) as dag:

    task_0 = BashOperator(task_id = 'bash_task', bash_command = "echo 'command executed from Bash Operator' ")
    task_1 = BashOperator(task_id ='bash_task_move_data', bash_command = "cp /Users/yasminbarree/DATA_CENTER/DATA_LAKE/DATASET_RAW.txt /Users/yasminbarree/DATA_CENTER/CLEAN_DATA  ")
    task_2 = BashOperator(task_id="bash_task_delete" , bash_command= "rm"+target+ "DATASET_RAW.txt")
task_0 >> task_1 >> task_2
