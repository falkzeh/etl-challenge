from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from extract import PopcoreChallenge


def run_popcore_challenge(url: str):
    etl = PopcoreChallenge(url=url)
    etl.execute()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 4, 27),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "popcore_challenge",
    default_args=default_args,
    description="A DAG to run PopcoreChallenge",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

owid_covid_data = PythonOperator(
    task_id="owid_covid_data",
    python_callable=run_popcore_challenge,
    op_args=["https://covid.ourworldindata.org/data/owid-covid-data.csv"],
    dag=dag,
)

covid_hospitalizations = PythonOperator(
    task_id="covid_hospitalizations",
    python_callable=run_popcore_challenge,
    op_args=["https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/hospitalizations/covid-hospitalizations.csv"],
    dag=dag,
)

owid_covid_data >> covid_hospitalizations
