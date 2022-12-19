import requests
import time
import json
import pandas as pd 
from airflow import DAG
import numpy as np
import os
from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator

# Define the API key outside of the function so it can be used in multiple requests
api_key = 'GLB0E7DZNVDFGHD3'

# Define the path to the DATA_LAKE folder
path = "/Users/yasminbarree/DATA_CENTER/DATA_LAKE"

def get_data(**kwargs):
    # Define the URLs with the correct API key
    url1 = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=' + api_key
    url2 = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=TSCO.LON&outputsize=full&apikey=' + api_key
    url3 = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SHOP.TRT&outputsize=full&apikey=' + api_key

    # Create a list of URLs
    urls = [url1, url2, url3]

    # Iterate through the list of URLs
    for url in urls:
        # Make request to the URL
        r = requests.get(url)
        try:  
            data = r.json()
            # Save the data to a file in the DATA_LAKE folder
            with open(os.path.join(path, "market_data.json"), "w") as f:
                json.dump(data, f)
        except:
            print("Error with API request")

# Set the default arguments for the DAG
default_dag_args = {
    'start_date': datetime(2022, 12, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1
}

# Create the DAG using the default arguments and a schedule interval of daily
with DAG("market_data_dag_2", schedule_interval = '@daily', catchup=False, default_args = default_dag_args) as dag_python:
    # Create a PythonOperator task to run the get_data function
    task_0 = PythonOperator(task_id = "get_market_data", python_callable = get_data)
