

import time

from datetime import timedelta
from airflow import DAG

from airflow.operators.postgres_operator import PostgresOperator 



from airflow.utils.dates import days_ago


default_dag_args = {
'owner': 'airflow',
'retries': 1,
'retry_delay': timedelta (minutes=5),

}

#sql logic defined

create_query = """
DROP TABLE IF EXISTS employee_table;
CREATE TABLE employee_table(name NOT NULL, age INT));

"""
# create a logic that populates the table with some data
insert_data_query = """
INSERT INTO employee_table((name, age INT) 
values ('name1', 40), ('name2' ,30), ('name3', 25)

"""



calculating_averag_age =  """ 
DROP TABLE IF EXISTS ticker_aggregated_data;
CREATE TABLE IF NOT EXISTS avg_age AS
SELECT avg(age)
FROM employee_table;


"""




dag_postgres = DAG(dag_id = 'postgres_dag_connection', default_args= default_dag_args,schedule_interval = None,start_date = days_ago(1) )

create_table = PostgresOperator(task_id = "creation_of_table",
    sql = create_query,
    dag = dag_postgres,postgres_conn_id = "postgres_local"  )


insert_data = PostgresOperator(task_id = "insertion_of_data",
    sql = insert_data_query,
    dag = dag_postgres,postgres_conn_id = "postgres_local"  )


group_data = PostgresOperator(task_id = "creating_avg_table",
    sql = calculating_averag_age,
    dag = dag_postgres,postgres_conn_id = "postgres_local"  )



create_table >> insert_data >> group_data