from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'royh',
    'start_date': datetime(2020, 5, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': True,
    'email_on_retry': False
}


dag = DAG(
    'scheduler_testing',
    default_args=default_args,
    schedule_interval='@daily'
)

t0 = BashOperator(
    task_id='where_am_i',
    bash_command='pwd && cd ~ && ls',
    dag=dag
)

t0 