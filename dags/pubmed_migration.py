from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta
import os
import logging
import fnmatch
import json
import glob
from pymongo.errors import BulkWriteError
from typing import List
from airflow.contrib.hooks.mongo_hook import MongoHook
from common.utils import *
from elasticsearch import Elasticsearch

# Setting up boto3 hook to AWS S3
s3_hook = S3Hook('my_conn_S3')
# Setting up MongoDB hook to mlab server
mongodb_hook = MongoHook('mongo_default')
ftp_conn_id = "pubmed_ftp"


def extract_pubmed_data() -> None:
    """Extracts case-reports from pubmed data and stores result on S3
    """
    pattern = "*.xml.tar.gz"
    ftp_path = '/pub/pmc/oa_bulk'
    root_dir = '/usr/local/airflow'
    temp_dir = os.path.join(root_dir, 'temp')
    bucket_name = 'supreme-acrobat-data'
    prefix = 'case_reports/pubmed/original'

    # deleting old entries in the bucket
    dest_path = 'case_reports/pubmed/original/'
    wildcard = 'case_reports/pubmed/original/*.*'
    old_klist = s3_hook.list_keys(bucket_name, prefix=dest_path, delimiter='/')
    if isinstance(old_klist, list):
        old_kmatches = [k for k in old_klist if fnmatch.fnmatch(k, wildcard)]
        if len(old_kmatches) > 0:
            s3_hook.delete_objects(bucket_name, old_kmatches)

    ftp_hook = ftp_connect(ftp_conn_id)
    filenames = ftp_hook.list_directory(ftp_path)
    ftp_disconnect(ftp_hook)
    filenames = list(
        filter(lambda filename: fnmatch.fnmatch(filename, pattern), filenames))

    for filename in filenames:
        ftp_hook = ftp_connect(ftp_conn_id)
        create_dir(temp_dir)
        remote_path = os.path.join(ftp_path, filename)
        local_path = os.path.join(temp_dir, filename)

        ftp_hook.retrieve_file(remote_path, local_path)
        o_path = extract_original_name(local_path)

        extract_file_case_reports(local_path, o_path)
        delete_file(local_path)
        make_tarfile(local_path, o_path)
        key = os.path.join(prefix, filename)
        s3_hook.load_file(
            local_path, key, bucket_name=bucket_name, replace=True)
        delete_dir(temp_dir)

        ftp_disconnect(ftp_hook)


def extract_pubmed_data_failure_callback(context) -> None:
    delete_temp()


def transform_pubmed_data() -> None:
    """Downloads tarfile from S3, forms JSON files from contents, and uploads to S3
    """
    root_dir = '/usr/local/airflow'
    temp_dir = os.path.join(root_dir, 'temp')
    src_path = 'case_reports/pubmed/original/'
    dest_path = 'case_reports/pubmed/json/'
    src_bucket_name = 'supreme-acrobat-data'
    dest_bucket_name = 'supreme-acrobat-data'
    wildcard_key = 'case_reports/pubmed/original/*.*'
    klist = s3_hook.list_keys(src_bucket_name, prefix=src_path, delimiter='/')
    key_matches = [k for k in klist if fnmatch.fnmatch(k, wildcard_key)]
    create_dir(temp_dir)
    filecount = 0

    # deleting old entries in the JSON folder
    wildcard = 'case_reports/pubmed/json/*.*'
    old_klist = s3_hook.list_keys(
        dest_bucket_name, prefix=dest_path, delimiter='')
    if isinstance(old_klist, list):
        old_kmatches = [k for k in old_klist if fnmatch.fnmatch(k, wildcard)]
        if len(old_kmatches) > 0:
            s3_hook.delete_objects(dest_bucket_name, old_kmatches)

    for key in key_matches:
        filecount += 1
        basename = os.path.basename(key)
        local_path = os.path.join(temp_dir, basename)
        o_path = extract_original_name(local_path)
        o_path_basename = os.path.basename(o_path)

        obj = s3_hook.get_key(key, src_bucket_name)
        obj.download_file(local_path)
        extract_file(local_path, temp_dir)
        glob_path = os.path.join(o_path, "*", "*.nxml")

        filenames = [f for f in glob.glob(glob_path)]
        json_path = os.path.join(temp_dir, 'temp' + str(filecount) + '.json')
        join_json_data(filenames, json_path)

        if (os.path.exists(json_path)):
            key = os.path.join(dest_path, os.path.basename(
                o_path_basename + ".json"))
            s3_hook.load_file(
                json_path, key, bucket_name=dest_bucket_name, replace=True)

    delete_dir(temp_dir)


def transform_pubmed_data_failure_callback(context) -> None:
    delete_temp()


def update_mongo() -> None:
    """Updates MongoDB caseReports
    """
    src_path = 'case_reports/pubmed/json/'
    src_bucket_name = 'supreme-acrobat-data'
    wildcard_key = 'case_reports/pubmed/json/*.*'

    klist = s3_hook.list_keys(src_bucket_name, prefix=src_path, delimiter='/')
    key_matches = [k for k in klist if fnmatch.fnmatch(k, wildcard_key)]

    for key in key_matches:
        docs = []
        obj = s3_hook.get_key(key, src_bucket_name)
        file_content = obj.get()['Body'].read().decode('utf-8')

        for line in file_content.splitlines():
            json_content = json.loads(line)
            docs.append(json_content)

        collection = 'casereports'
        filter_docs = [{'pmID': doc['pmID']} for doc in docs]

        try:
            result = mongo_insert(
                mongodb_hook, collection, docs, filter_docs)
        except BulkWriteError as bwe:
            logging.info(bwe.details)
            logging.info(bwe.details['writeErrors'])

            raise bwe

        logging.info(result.upserted_ids)
        _ids = list(str(v) for v in result.upserted_ids.values())
        # add casereport _id to docs
        for i, doc in enumerate(docs):
            doc["id"] = _ids[i]
        # xcom push by return 
        return docs


def update_elasticsearch() -> None:
    es = Elasticsearch(
        ['https://search-acrobat-smsvp2rqdw7jhssq3selgvrqyi.us-west-2.es.amazonaws.com'])

    data = ti.xcom_pull(key=None, task_ids='update_mongo')
    count = int(es.cat.count(index='casereport').strip().split(' ')[2])

    body = generate_elasticsearch_body(count, data)

    es.bulk(body=body)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 2, 1),
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'pubmed_migration_AWS',
    default_args=default_args,
    description='Pubmed Migration',
    schedule_interval='@once',
)

extract_pubmed_data_task = PythonOperator(
    task_id='extract_pubmed_data',
    python_callable=extract_pubmed_data,
    on_failure_callback=extract_pubmed_data_failure_callback,
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

update_elasticsearch_task = PythonOperator(
    task_id='update_elasticsearch',
    python_callable=update_elasticsearch,
    dag=dag,
)

extract_pubmed_data_task >> transform_pubmed_data_task >> update_mongodb_task
