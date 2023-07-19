import json
import pathlib

import airflow
import requests
import requests.exceptions as requests_exception
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id = "download_rocket_launches",
    start_date = airflow.utils.dates.days_ag(14),
    schedule_interval = None
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command = "curl -o /tmp/launches.json -L
                   'https://ll.thespacedevs.com/2.0.0/launch/upcoming'", dag=dag,
)