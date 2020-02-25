#import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import airflow.hooks.S3_hook
import datetime
import os

THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
my_file = os.path.join(THIS_FOLDER, 'test.txt')

def upload_file_to_S3_with_hook(filename, key, bucket_name):
    hook = airflow.hooks.S3_hook.S3Hook('my_conn_S3')
    hook.load_file(filename, key, bucket_name)

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2020, 2, 1),
    'retry_delay': datetime.timedelta(minutes=5)
}

#files = files("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/*.xml.tar.gz")
    #"ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/non_comm_use.A-B.xml.tar.gz"
#    for file in files:
        # download file
        # uncompress
        # untar
        # delete non-case-reports
        # tar
        # compress
#        upload_file_to_S3_with_hook(filename, key, bucket_name)

with DAG('S3_dag_test', default_args=default_args, schedule_interval='@once') as dag:

    start_task = DummyOperator(
            task_id='dummy_start'
    )

    upload_to_S3_task = PythonOperator(
            task_id='upload_file_to_S3',
            python_callable=lambda: print("Uploading file to S3")
    )

    start_task >> upload_to_S3_task

upload_to_S3_task = PythonOperator(
    task_id='upload_file_to_S3_with_hook',
    python_callable=upload_file_to_S3_with_hook,
    op_kwargs={
        'filename': my_file,
        'key': 'new3.txt',
        'bucket_name': 'supreme-acrobat',
    },
    dag=dag)
