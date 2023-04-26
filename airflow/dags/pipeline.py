from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id='get_covid_data',
    start_date=datetime(2023, 4, 26),
    schedule_interval=None
) as dag:
    extract = BashOperator(
        task_id='extract',
        bash_command='python3 extract.py',
    )

