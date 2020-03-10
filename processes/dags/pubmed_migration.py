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
import json
import pubmed_parser as pp

# Setting up boto3 hook to AWS S3
s3_hook = S3Hook('my_conn_S3')
# Setting up FTP hook to pubmed ftp server
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


def extract_file(filepath: str, dest_dir: str, filter_pattern: str = None) -> None:
    """Extracts tar.gz file.

    Args:
        filepath (str): path of file.
        dest_dir (str): destination directory.
        filter_pattern (str): extracts only files matching filter_pattern.
    """
    my_tar = tarfile.open(filepath)
    for member in my_tar.getmembers():
        if filter_pattern:
            if filter_pattern in member.name:
                my_tar.extract(member, dest_dir)
        else:
            my_tar.extract(member, dest_dir)
    my_tar.close()


def extract_file_case_reports(filepath: str, dest_dir: str) -> None:
    """Extracts only case reports from tar.gz file.

    Args:
        filepath (str): path of file.
        dest_dir (str): destination directory.
    """
    extract_file(filepath, dest_dir, "Case_Rep")


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
    """Deletes temporary directory that processes files
    """
    root_dir = '/usr/local/airflow'
    temp_dir = root_dir + '/' + 'temp'
    delete_dir(temp_dir)


def make_jsonfile(obj: dict, output_filepath: str) -> None:
    """Make a JSON file from the casereport dictionary

    Args:
        obj (dict): casereport XML parsed as a dictionary
        output_filepath (str): path of output file.
    """
    with open(output_filepath, 'w') as json_file:
        json.dump(obj, json_file)


def make_case_report_json(xml_path: str, json_path: str) -> None:
    """Makes a JSON file from pubmed XML files

    Args:
        xml_path (str): path to input XML file
        json_path (str): path to destination JSON file
    """
    pubmed_xml = pp.parse_pubmed_xml(xml_path)
    pubmed_paragraph = pp.parse_pubmed_paragraph(xml_path)
    pubmed_references = pp.parse_pubmed_references(xml_path)

    text = " ".join([p["text"] for p in pubmed_paragraph])
    case_report = {
        "pmID": pubmed_xml["pmid"],
        "messages": [],
        "source_files": [],
        "modifications": [],
        "normalizations": [],
        # ctime            : 1351154734.5055847,
        "text": text,
        "entities": [],
        "attributes": [],
        # date : { type: Date, default: Date.now }
        "relations": [],
        "triggers": [],
        "events": [],
        "equivs": [],
        "comments": [],
        # sentence_offsets     : [],
        # token_offsets    : [],
        "action": None,
        "abstract": pubmed_xml["abstract"],
        "authors": pubmed_xml["author_list"],
        "keywords": [],
        "introduction": None,
        "discussion": None,
        "references": []
    }

    make_jsonfile(case_report, json_path)


def extract_pubmed_data() -> None:
    """Extracts case-reports from pubmed data and stores result on S3
    """

    #to test specific tar files
    #pattern = 'non_comm_use.A-B.xml.tar.gz'
    pattern = "*.xml.tar.gz"
    ftp_path = '/pub/pmc/oa_bulk'
    root_dir = '/usr/local/airflow'
    temp_dir = os.path.join(root_dir, 'temp')
    bucket_name = 'supreme-acrobat-data'
    prefix = 'case_reports/pubmed/original'

    filenames = ftp_hook.list_directory(ftp_path)
    filenames = list(
        filter(lambda filename: fnmatch.fnmatch(filename, pattern), filenames))

    for filename in filenames:
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


def extract_pubmed_data_failure_callback(context) -> None:
    delete_temp()


def transform_pubmed_data() -> None:
    """Downloads tarfile from S3, forms JSON files from contents, and uploads to S3
    """
    root_dir = '/usr/local/airflow'
    temp_dir = os.path.join(root_dir, 'temp')
    json_dir = os.path.join(temp_dir, 'json')
    src_path = 'case_reports/pubmed/original/'
    dest_path = 'case_reports/pubmed/json/'
    src_bucket_name = 'supreme-acrobat-data'
    dest_bucket_name = 'supreme-acrobat-data'
    wildcard_key = 'case_reports/pubmed/original/*.*'
    klist = s3_hook.list_keys(src_bucket_name, prefix='case_reports/pubmed/original/', delimiter='/')
    key_matches = []
    key_matches = [k for k in klist if fnmatch.fnmatch(k, wildcard_key)]

    logging.info(key_matches)

    for key in key_matches:
        create_dir(temp_dir)
        create_dir(json_dir)

        basename = os.path.basename(key)
        local_path = os.path.join(temp_dir, basename)
        o_path = extract_original_name(local_path)

        obj = s3_hook.get_key(key, src_bucket_name)
        obj.download_file(local_path)

        extract_file(local_path, temp_dir)

        glob_path = os.path.join(o_path, "*", "*.nxml")

        filenames = [f for f in glob.glob(glob_path)]
        logging.info("filenames = %s" % filenames)

        for filename in filenames[0:2]:
            fname, ext = os.path.splitext(os.path.basename(filename))
            json_filename = ".".join((fname, "json"))
            json_path = os.path.join(json_dir, json_filename)
            make_case_report_json(filename, json_path)

        jsons = [f for f in glob.glob(os.path.join(json_dir, "*.json"))]

        logging.info("jsons = %s" % jsons)

        for filename in jsons:
            key = os.path.join(dest_path, os.path.basename(filename))
            s3_hook.load_file(filename, key,
                              bucket_name=dest_bucket_name, replace=True)

        delete_dir(temp_dir)


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
    # on_failure_callback=extract_pubmed_data_failure_callback,
    dag=dag,
)

transform_pubmed_data_task = PythonOperator(
    task_id='transform_pubmed_data',
    python_callable=transform_pubmed_data,
    on_failure_callback=transform_pubmed_data_failure_callback,
    dag=dag,
)

# # Create MongoDB snapshot and upload to S3
create_snapshot_task = DummyOperator(
    task_id='create_snapshot',
    dag=dag,
)

# # Download json files from s3
# # Update mongodb.
update_mongodb_task = DummyOperator(
    task_id='update_mongodb',
    dag=dag,
)

extract_pubmed_data_task >> transform_pubmed_data_task >> create_snapshot_task >> update_mongodb_task
