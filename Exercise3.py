
import requests
import time
import json
import pandas as pd 
from airflow import DAG
import numpy as np
import os

from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

#to make your function accept parameters , you must define key arguments -> kwargs
#by writing kwargs your airflow task is expecting dictionary or arguments 

def get_data(**kwargs):
    api_key = 'GLB0E7DZNVDFGHD3'

    # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo' + api_key
    r = requests.get(url)

    try:  
        data = r.json()
        path = "/Users/yasminbarree/DATA_CENTER/DATA_LAKE/"
        with open(path + "stock _market_raw_data" + "IBM_" + str(time. time()), "W") as outfile:
            json.dump(data, outfile)
    except:
        pass

def test_data(**kwargs):
    pass


def clean_data(**kwargs):
  read_path = "/Users/yasminbarree/DATA_CENTER/DATA_LAKE/"
    #get a way to read the most file from dictory of data 
  ticker = kwargs['ticker']
  lastest = np.max([float(file.split('_')[-1]) for file in os.listdir(read_path) if ticker in file])
  latest_file = [file for file in os.listdir(read_path) if str(latest) in file ][0]


  output_path = "/Users/yasminbarree/DATA_CENTER/CLEAN_DATA/"

  #loading the raw data from the file 
  file = open(read_path + latest_file)
  data = json.load(file)


  
  clean_data = pd.DataFrame(data['Time Series (Daily)']).T 
  clean_data['ticker'] = data['Meta Data']['2. Symbol']
  clean_data['Meta Data'] = str(data['Meta Data'])
  clean_data['timestamp'] = pd.to_datetime('now')

  # storing the data in the clean data lake 
  clean_data.to_csv(output_path + 'IBM_snapshot_daily' + str(pd.to_datetime('now')) + '.csv')
  


default_dag_args = {
'start_date': datetime (2022, 12, 15),
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
'retry_delay': timedelta (minutes=5),
'project_id': 1

}

with DAG("market_data_dag", schedule_interval = '@daily', catchup=False, default_args = default_dag_args) as dag_python:



    task_0 = PythonOperator(task_id = "get_market_data", python_callable = get_data,op_kwargs = {'ticker' :'IBM'})
task_0
