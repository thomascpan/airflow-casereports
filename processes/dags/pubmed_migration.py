from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.ftp_hook import FTPHook
import datetime
import os
import logging
import tarfile
import ntpath
import shutil
import fnmatch
import glob
import pubmed_parser as pp


s3_hook = S3Hook('my_conn_S3')
ftp_hook = FTPHook('pubmed_ftp')


def extract_original_name(filepath: str) -> str:
    """Extracts original name from tar.gz file

    Args:
        filepath (str): path of file.

    Returns:
        str: The original name
    """
    name = filepath.split(".")
    return ".".join(name[0:-2])


def create_dir(dirname: str) -> None:
    """Creates directory even if exist

    Args:
        dirname (str): name of directory.
    """
    if not os.path.exists(dirname):
        os.mkdir(dirname)


def delete_dir(dirname: str) -> None:
    """Deletes directory even if exist

    Args:
        dirname (str): name of directory.
    """
    shutil.rmtree(dirname)


def extract_file(filepath: str, dest_dir: str) -> None:
    """Extracts tar.gz file.

    Args:
        filepath (str): path of file.
        dest_dir (str): destination directory.
    """
    my_tar = tarfile.open(filepath)
    for member in my_tar.getmembers():
        if "Case_Rep" in member.name:
            my_tar.extract(member, dest_dir)
    my_tar.close()


def delete_file(filepath: str) -> None:
    """Delete file.

    Args:
        filepath (str): path of file.
    """
    if os.path.exists(filepath):
        os.remove(filepath)


def make_tarfile(output_filepath: str, source_dir: str) -> None:
    """Make tar.gz file.

    Args:
        output_filepath (str): path of output file.
        dest_dir (str): source directory.
    """
    with tarfile.open(output_filepath, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

def delete_temp() -> None:
    root_dir = '/usr/local/airflow'
    temp_dir = root_dir + '/' + 'temp'
    delete_dir(temp_dir)    


def extract_pubmed_data() -> None:
    """Extracts case-reports from pubmed data and stores result on S3
    """
    pattern = "*.xml.tar.gz"
    ftp_path = '/pub/pmc/oa_bulk'
    root_dir = '/usr/local/airflow'
    temp_dir = os.path.join(root_dir, 'temp')
    bucket_name = 'supreme-acrobat-data/case_reports/pubmed/original'

    filenames = ftp_hook.list_directory(ftp_path)
    filenames = list(
        filter(lambda filename: fnmatch.fnmatch(filename, pattern), filenames))

    for filename in filenames:
        create_dir(temp_dir)
        remote_path = os.path.join(ftp_path, filename)
        local_path = os.path.join(temp_dir, filename)

        ftp_hook.retrieve_file(remote_path, local_path)
        o_path = extract_original_name(local_path)

        extract_file(local_path, o_path)
        delete_file(local_path)
        make_tarfile(local_path, o_path)
        s3_hook.load_file(local_path, filename,
                          bucket_name=bucket_name, replace=True)
        delete_dir(temp_dir)


def extract_pubmed_data_failure_callback(context) -> None:
    delete_temp()


def transform_pubmed_data() -> None:
    root_dir = '/usr/local/airflow'
    temp_dir = os.path.join(root_dir, 'temp')
    src_path = 'case_reports/pubmed/original/'
    dest_path = 'case_reports/pubmed/json/'
    src_bucket_name = 'supreme-acrobat-data'
    dest_bucket_name = 'supreme-acrobat-data'

    keys = s3_hook.list_keys(src_bucket_name, src_path, '/')

    logging.info(keys)

    # for key in keys:
    #     create_dir(temp_dir)
    #     local_path = os.path.join(temp_dir, key)

    #     obj = s3_hook.get_key(key, src_bucket_name)
    #     obj.download_file(local_path)
    #     o_path = extract_original_name(local_path)

    #     extract_file(local_path, o_path)
    #     delete_file(local_path)

    #     filenames = [f for f in glob.glob(o_path + "*/*.nxml")]

    #     for filename in filenames[0:2]:
    #         dict_out = pp.parse_pubmed_xml(filename)
    #         print(dict_out)


            # s3_hook.load_file(local_path, filename,
            #                   bucket_name=dest_bucket_name, replace=True)

        # delete_dir(temp_dir)



    raise Exception()

def transform_pubmed_data_failure_callback(context) -> None:
    delete_temp()


default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2020, 2, 1),
    'retry_delay': datetime.timedelta(minutes=5)
}

dag = DAG(
    'pubmed_migration',
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

# Download from S3
# Extract relevant values and create json files following case_report.js schema.
#   i.e. PMC4716828.json
#   Use pubmed parser: https://github.com/titipata/pubmed_parser
# Upload to s3.
transform_pubmed_data_task = PythonOperator(
    task_id='transform_pubmed_data',
    python_callable=transform_pubmed_data,
    on_failure_callback=transform_pubmed_data_failure_callback,
    dag=dag,
)

# Create MongoDB snapshot and upload to S3
create_snapshot_task = DummyOperator(
    task_id='create_snapshot',
    dag=dag,
)

# Download json files from s3
# Update mongodb.
update_mongodb_task = DummyOperator(
    task_id='update_mongodb',
    dag=dag,
)

extract_pubmed_data_task >> transform_pubmed_data_task >> create_snapshot_task >> update_mongodb_task
