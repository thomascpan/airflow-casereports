#import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import airflow.hooks.S3_hook
import datetime
import os
import logging
import urllib
from ftplib import FTP
import tarfile

THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
my_file = os.path.join(THIS_FOLDER, 'test.txt')

def upload_file_to_S3_with_hook(filename, key, bucket_name):
    hook = airflow.hooks.S3_hook.S3Hook('my_conn_S3')
    hook.load_file(filename, key, bucket_name)

def import_files():
    logging.info("start import files")
    path = 'pub/pmc/oa_bulk'
    root_dir = '/usr/local/airflow'
    temp_dir = root_dir + '/' + 'temp'
    files = []
    #filenames = ftp.nlst('pub/pmc/oa_bulk/*.xml.tar.gz')

    ftp = FTP("ftp.ncbi.nlm.nih.gov")
    ftp.login("anonymous", "ifso6888@gmail.com")
    ftp.cwd('pub/pmc/oa_bulk/')
    filenames = ftp.nlst('non_comm_use.A-B.xml.tar.gz')
    ftp.dir(files.append)
    if not os.path.exists(temp_dir):
        os.mkdir(temp_dir)
    for filename in filenames:
        local_filename = os.path.join(temp_dir, filename)
        file = open(local_filename, 'wb')
        ftp.retrbinary("RETR " + filename, file.write)
        file.close()
        my_tar = tarfile.open(local_filename)
        for member in my_tar.getmembers():
            if "Case_Rep" in member.name:
                my_tar.extract(member, temp_dir)
        my_tar.close()
    ftp.quit()
    os.rmdir(temp_dir)
    logging.info("done with files download")
    #upload_file_to_S3_with_hook(my_file, 'newfile.txt', 'supreme-acrobat')
    return None

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2020, 2, 1),
    'retry_delay': datetime.timedelta(minutes=5)
}

with DAG('S3_dag_test', default_args=default_args, schedule_interval='@once') as dag:

    start_task = DummyOperator(
            task_id='dummy_start'
    )

    import_files_task = PythonOperator(
        task_id = 'import_files',
        python_callable=import_files
    )

    start_task >> import_files_task
