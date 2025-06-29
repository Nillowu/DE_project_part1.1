from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from scripts.py_for_dags import (log_start, log_end, load_data_to_FT_BALANCE_F, load_data_to_FT_POSTING_F,
load_data_to_MD_ACCOUNT_D, load_data_to_MD_CURRENCY_D, load_data_to_MD_EXCHANGE_RATE_D, load_data_to_MD_LEDGER_ACCOUNT_S)


default_args = {
    'owner': 'olesya',
    'depends_on_past': False,
    'start_date': datetime(2025, 6 ,25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG (
    dag_id = 'bank_etl_process',
    default_args = default_args,
    schedule= None,
    tags = ['bank']
)

def start_logs():
    log_id = log_start('bank_etl_process')
    time.sleep(5)
    return log_id

def end_logs(**kwargs):
    ti = kwargs['ti']
    log_id = ti.xcom_pull(task_ids = 'start_logs')
    log_end(log_id, status = 'SUCCESS')


start_log_task = PythonOperator(
    task_id = 'start_logs',
    python_callable = start_logs,
    dag = dag,
)

load_ft_balance_f_task = PythonOperator(
    task_id = 'load_ft_balance_f',
    python_callable = load_data_to_FT_BALANCE_F,
    dag = dag,
)

load_ft_posting_f_task = PythonOperator(
    task_id = 'load_ft_posting_f',
    python_callable = load_data_to_FT_POSTING_F,
    dag = dag,
)

load_md_account_d_task = PythonOperator(
    task_id = 'load_md_account_d',
    python_callable = load_data_to_MD_ACCOUNT_D,
    dag = dag,
)

load_md_currency_d_task = PythonOperator(
    task_id = 'load_md_currency_d',
    python_callable = load_data_to_MD_CURRENCY_D,
    dag = dag,
)

load_md_exchange_rate_d_task = PythonOperator(
    task_id = 'load_md_exchange_rate_d',
    python_callable = load_data_to_MD_EXCHANGE_RATE_D,
    dag = dag,
)

load_md_ledger_account_s_task = PythonOperator(
    task_id = 'load_md_ledger_account_s',
    python_callable = load_data_to_MD_LEDGER_ACCOUNT_S,
    dag = dag,
)

end_log_task = PythonOperator(
    task_id = 'end_logs',
    python_callable = end_logs,
    dag = dag,
)

start_log_task >> [load_md_account_d_task, load_md_exchange_rate_d_task, load_md_currency_d_task, load_md_ledger_account_s_task, load_ft_posting_f_task, load_ft_balance_f_task] >> end_log_task