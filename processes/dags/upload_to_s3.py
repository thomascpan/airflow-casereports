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


THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
my_file = os.path.join(THIS_FOLDER, 'test.txt')

def upload_file_to_S3_with_hook(filename, key, bucket_name):
    hook = airflow.hooks.S3_hook.S3Hook('my_conn_S3')
    hook.load_file(filename, key, bucket_name)

def import_wrapper():
    logging.info("start import wrapper")
    download_files()
    #decompress_files()
    #delete_extra_files()
    #compress_files()
    return None
    #upload_file_to_S3_with_hook(my_file, 'newfile.txt', 'supreme-acrobat')

def download_files():
    #path = 'pub/pmc/oa_bulk'
    #files = []
    # filenames = ftp.nlst('pub/pmc/oa_bulk/*.xml.tar.gz')
    ftp = FTP("ftp.ncbi.nlm.nih.gov")
    ftp.login("anonymous", "ifso6888@gmail.com")
    #ftp.cwd(path)
    #filenames = ftp.nlst('pub/pmc/oa_bulk/comm_use.A-B.xml.tar.gz')
    #ftp.dir(files.append)
    #logging.info(files)

    #for filename in filenames:
        #ftp.open(filename, 'wb').write)
        #ftp.retrbinary("RETR " + filename)
    ftp.quit()
    #upload_file_to_S3_with_hook(my_file, 'newfile2.txt', 'supreme-acrobat')
    logging.info("done download")
    return None

def decompress_files():
    pass

def delete_extra_files():
    pass

def compress_files():
    pass

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2020, 2, 1),
    'retry_delay': datetime.timedelta(minutes=5)
}

#files = files("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/*.xml.tar.gz")
    #"ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/non_comm_use.A-B.xml.tar.gz"
#    for file in files:
        # download file
        # untar
        # delete non-case-reports
        # tar
        # compress
#        upload_file_to_S3_with_hook(filename, key, bucket_name)

with DAG('S3_dag_test', default_args=default_args, schedule_interval='@once') as dag:

    start_task = DummyOperator(
            task_id='dummy_start'
    )

    import_files_task = PythonOperator(
        task_id = 'import_wrapper',
        python_callable=import_wrapper
    )

    start_task >> import_files_task
