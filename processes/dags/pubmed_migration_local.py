from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import logging
import fnmatch
import glob
import json
from utils import *


def extract_pubmed_data() -> None:
    """Extracts case-reports from pubmed data and stores result on local server
    """
    # to test specific tar files
    pattern = 'non_comm_use.A-B.xml.tar.gz'
    #pattern = "*.xml.tar.gz"
    ftp_path = '/pub/pmc/oa_bulk'
    root_dir = '/usr/local/airflow'
    pubmed_dir = os.path.join(root_dir, 'pubmed')
    original_dir = os.path.join(pubmed_dir, 'original')
    prefix = 'test/pubmed/original'

    delete_dir(original_dir)
    create_dir(pubmed_dir)
    create_dir(original_dir)

    ftp_hook = ftp_connect(ftp_conn_id)
    filenames = ftp_hook.list_directory(ftp_path)
    ftp_disconnect(ftp_hook)
    filenames = list(
        filter(lambda filename: fnmatch.fnmatch(filename, pattern), filenames))

    for filename in filenames:
        ftp_hook = ftp_connect(ftp_conn_id)
        remote_path = os.path.join(ftp_path, filename)
        local_path = os.path.join(original_dir, filename)

        ftp_hook.retrieve_file(remote_path, local_path)
        o_path = extract_original_name(local_path)

        extract_file_case_reports(local_path, o_path)
        delete_file(local_path)
        make_tarfile(local_path, o_path)
        delete_dir(o_path)

        ftp_disconnect(ftp_hook)


def extract_pubmed_data_failure_callback(context) -> None:
    pass


def transform_pubmed_data() -> None:
    """Downloads forms JSON files from contents of tarfile
    """
    root_dir = '/usr/local/airflow'
    pubmed_dir = os.path.join(root_dir, 'pubmed')
    json_dir = os.path.join(pubmed_dir, 'json')
    original_dir = os.path.join(pubmed_dir, 'original')

    delete_dir(json_dir)
    create_dir(pubmed_dir)
    create_dir(json_dir)

    file_path = os.path.join(original_dir, "*.xml.tar.gz")
    filenames = [f for f in glob.glob(file_path)]

    for filename in filenames:
        basename = os.path.basename(filename)
        local_path = os.path.join(original_dir, basename)
        dest_path = os.path.join(json_dir, basename)
        o_path = extract_original_name(dest_path)
        o_path_basename = os.path.basename(o_path)
        extract_file(local_path, json_dir)
        glob_path = os.path.join(o_path, "*", "*.nxml")
        filenames = [f for f in glob.glob(glob_path)]
        json_path = os.path.join(json_dir, o_path_basename + '.json')
        join_json_data(filenames, json_path)
        delete_dir(o_path)


def transform_pubmed_data_failure_callback(context) -> None:
    pass


def update_mongo() -> None:
    """Updates MongoDB caseReports
    """
    root_dir = '/usr/local/airflow'
    pubmed_dir = os.path.join(root_dir, 'pubmed')
    json_dir = os.path.join(pubmed_dir, 'json')

    file_path = os.path.join(json_dir, "*.json")
    filenames = [f for f in glob.glob(file_path)]

    for filename in filenames:
        docs = []
        with open(filename) as f:
            for line in f:
                docs.append(json.loads(line))

        collection = 'caseReports'
        filter_docs = [{'pmID': doc['pmID']} for doc in docs]
        mongo_insert(collection, docs, filter_docs)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 2, 1),
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'pubmed_migration_local',
    default_args=default_args,
    description='Pubmed Migration',
    schedule_interval='@once',
)

extract_pubmed_data_task = PythonOperator(
    task_id='extract_pubmed_data',
    python_callable=extract_pubmed_data,
    dag=dag,
)

transform_pubmed_data_task = PythonOperator(
    task_id='transform_pubmed_data',
    python_callable=transform_pubmed_data,
    on_failure_callback=transform_pubmed_data_failure_callback,
    dag=dag,
)

update_mongodb_task = PythonOperator(
    task_id='update_mongodb',
    python_callable=update_mongo,
    dag=dag,
)

extract_pubmed_data_task >> transform_pubmed_data_task >> update_mongodb_task
