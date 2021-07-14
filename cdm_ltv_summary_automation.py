import inspect
from datetime import timedelta, datetime, date
import json
import logging
import os, sys
# import slack
# from slack import WebClient

#airflow
from airflow import DAG, configuration, macros, models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator
#gcp
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

# slack
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.utils.db import provide_session
from airflow.utils.trigger_rule import TriggerRule

from airflow.models import Connection

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

APP_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(APP_DIR)
print(f'ROOT_DIR is -> {ROOT_DIR} & APP_DIR is -> {APP_DIR}')
sys.path.append(APP_DIR)
sys.path.append(ROOT_DIR)
sys.path.append('/'.join(ROOT_DIR.split('/')[:-1]))
from common_lib.utils_v1 import *
from main import main_ltv


BASE_URL = configuration.get('webserver', 'BASE_URL')
logging.basicConfig(level=logging.INFO)

DAG_ID = 'cdm_ltv_summary_process'
DAG_CONFIG_NAME = 'config_' + DAG_ID
GC_BQ_PROJECT = get_conn(DAG_CONFIG_NAME).extra_dejson.get('gc_bq_project')
# GC_GS_BUCKET = get_conn(DAG_CONFIG_NAME).extra_dejson.get('gc_gs_bucket')
TILL_DATE = get_conn(DAG_CONFIG_NAME).extra_dejson.get('till_date')
START_DATE = get_conn(DAG_CONFIG_NAME).extra_dejson.get('start_date')
# LOCAL_TMP_DIR = get_conn(DAG_CONFIG_NAME).extra_dejson.get('locl_tmp_dir')
SRC_SYSTEM_ID = get_conn(DAG_CONFIG_NAME).extra_dejson.get('src_system_id')
AD_REV_PER_SUBS = get_conn(DAG_CONFIG_NAME).extra_dejson.get('ad_rev_per_subs')
FORCED_RUN=get_conn(DAG_CONFIG_NAME).extra_dejson.get('forced_run')
DATASET_NAME=get_conn(DAG_CONFIG_NAME).extra_dejson.get('dataset_name')

default_args = {
    'owner': 'Anit Gupta',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 3),
    'email': ['anit.gupta@cbsinteractive.com'], #Change this generic
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

with DAG(dag_id=DAG_ID, schedule_interval='0 8 1 */3 *',
         default_args=default_args, catchup=False) as dag:
    slack_notify_done = SlackAPIPostOperator(
        task_id="slack_notify_done",
        username='Airflow',
        token=get_slack_token(),
        channel=SLACK_CHANNEL,
        attachments=[{"color": "good", "text": "*<" + BASE_URL
                                               + "/tree?dag_id={{ dag.dag_id }}|LTV SUMMARIZATION PROCESS> for {{ds}} SUCCESS!*"}],
        text='')

    run_ltv_summary_process = PythonOperator(
        task_id='run_ltv_summary_process',
        provide_context=True,
        python_callable=main_ltv,  # get_gcs_file,
        params={
            'GC_BQ_PROJECT': GC_BQ_PROJECT,
            'till_date': TILL_DATE,
            'start_date': START_DATE,
            'src_system_id': SRC_SYSTEM_ID,
            'ad_rev_per_subs': AD_REV_PER_SUBS,
            'dataset_name':DATASET_NAME,
            'forced_run': FORCED_RUN
            #'GC_BQ_STAGING_DS': GC_BQ_STAGING_DS,
            #'GC_GS_BUCKET': GC_GS_BUCKET,
            #'GCP_SERVICE_ACCOUNT_KEY': GCP_SERVICE_ACCOUNT_KEY,
            #'logical_dt': logical_dt_dash,
            #'APP_DIR': '/var/tmp/can_feed_file/',
            #'updated_records_days': updated_records_days,
            #'guid_list_from_table': False
        },
        retries=3,
        retry_delay=timedelta(seconds=10),
        depends_on_past=False,
        on_failure_callback=slack_failed_task,
    )

    run_ltv_summary_process >> slack_notify_done