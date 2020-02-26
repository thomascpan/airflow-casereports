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
import ntpath
import shutil


def upload_file_to_S3_with_hook(filename: str, key: str, bucket_name: str) -> None:
    """Uploads file to s3.

    Args:
        filename (str): path of file.
        key (str): S3 key that will point to the file.
        bucket_name (str): Name of the bucket in which to store the file.

    """
    hook = airflow.hooks.S3_hook.S3Hook('my_conn_S3')
    hook.load_file(filename, key, bucket_name=bucket_name, replace=True)


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


def download_file(ftp: FTP, filename: str, dest_dir: str) -> str:
    """Downloads file from FTP.

    Args:
        ftp (FTP): FTP instance.
        filename (str): name of file to download.
        dest_dir (str): destination directory.

    Returns:
        str: path of download file.
    """
    filepath = os.path.join(dest_dir, filename)
    file = open(filepath, 'wb')
    ftp.retrbinary("RETR " + filename, file.write)
    file.close()
    return filepath


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


def extract_pubmed_data() -> None:
    """Extracts case-reports from pubmed data and stores result on S3
    """
    logging.info("start import wrapper")
    username = "anonymous"
    password = "ifso6888@gmail.com"
    ftp_url = 'ftp.ncbi.nlm.nih.gov'
    ftp_path = 'pub/pmc/oa_bulk'
    root_dir = '/usr/local/airflow'
    temp_dir = root_dir + '/' + 'temp'

    ftp = FTP(ftp_url)
    ftp.login(username, password)
    ftp.cwd(ftp_path)
    filenames = ftp.nlst('pub/pmc/oa_bulk/*.xml.tar.gz')
    try:
        ftp.quit()
    except:
        None

    for filename in filenames:
        ftp = FTP(ftp_url)
        ftp.login(username, password)
        ftp.cwd(ftp_path)

        create_dir(temp_dir)
        filepath_tar_gz = download_file(ftp, filename, temp_dir)
        filename_tar_gz = ntpath.basename(filepath_tar_gz)
        o_path = extract_original_name(filepath_tar_gz)

        extract_file(filepath_tar_gz, o_path)
        delete_file(filepath_tar_gz)
        make_tarfile(filepath_tar_gz, o_path)
        upload_file_to_S3_with_hook(
            filepath_tar_gz, filename_tar_gz, 'supreme-acrobat-data')
        delete_dir(temp_dir)

        try:
            ftp.quit()
        except:
            None


def extract_pubmed_data_failure_callback() -> None:
    root_dir = '/usr/local/airflow'
    temp_dir = root_dir + '/' + 'temp'
    delete_dir(temp_dir)


default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2020, 2, 1),
    'retry_delay': datetime.timedelta(minutes=5)
}

dag = DAG(
    'S3_dag_test',
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

transform_pubmed_data_task = DummyOperator(
    task_id='transform_pubmed_data',
    dag=dag,
)

create_snapshot_task = DummyOperator(
    task_id='create_snapshot',
    dag=dag,
)

update_mongodb_task = DummyOperator(
    task_id='update_mongodb',
    dag=dag,
)

extract_pubmed_data_task >> transform_pubmed_data_task >> create_snapshot_task >> update_mongodb_task
