from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import requests
from functions.functions import post


def _test_api():
    request = requests.get("https://outstanding-toys-production.up.railway.app/data/")
    return request.status_code


def api_val(ti):
    res = ti.xcom_pull(task_ids="test_api")
    if res != 200:
        return 'dead_api'
    return 'post_api'

with DAG('dag_cam', start_date = datetime(2022,11,29),
    schedule_interval = '10 * * * *', catchup=False) as dag:

    #verify if site is up
        test_api = PythonOperator(
            task_id="test_api",
            python_callable=_test_api
        )

        api_val = BranchPythonOperator(
            task_id="api_val",
            python_callable=api_val
        )
    #run post on pyt
        post_api = PythonOperator(
            task_id="post_api",
            python_callable=post("https://outstanding-toys-production.up.railway.app/data/")
        )

    #else: return error sent
        dead_api = BashOperator(
            task_id="dead_api",
            bash_command="echo 'Dead API :('"
        )

        test_api >> api_val >> [post_api, dead_api]