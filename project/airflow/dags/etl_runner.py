"""
A runner of the ETL process.
"""
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

sys.path.insert(0, "/usr/local/airflow/app")

from run import start_app


with DAG('etl_runner',
         description='A runner for ETL tool',
         start_date=datetime(2022, 8, 26),
         schedule_interval=timedelta(days=1),
         max_active_runs=1,
         catchup=False) as dag:

    start = DummyOperator(task_id='start')

    end = DummyOperator(task_id='end')

    etl_task = PythonOperator(task_id='parquet_files_generator',
                              python_callable=start_app(),
                              dag=dag)

    start >> etl_task >> end
