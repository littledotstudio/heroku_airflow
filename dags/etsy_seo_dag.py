from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'royh',
    'start_date': datetime(2020, 5, 5),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}


dag = DAG(
    'seo_rankings',
    default_args=default_args,
    schedule_interval='@daily'
)

t0 = BashOperator(
    task_id='where_am_i',
    bash_command='pwd && cd ~ && ls',
    dag=dag
)

t1 = BashOperator(
    task_id='get_coaster_rankings',
    bash_command='cd ~ && cd dags && python etsy_coaster_seo.py',
    dag=dag
)

t2 = BashOperator(
    task_id='get_luggage_rankings',
    bash_command='cd ~ && cd dags && python etsy_luggagetag_seo.py',
    dag=dag
)

t0 >> t1 >> t2