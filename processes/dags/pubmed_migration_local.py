from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.contrib.hooks.mongo_hook import MongoHook
from datetime import datetime, timedelta
import os
import logging
import tarfile
import ntpath
import shutil
import fnmatch
import glob
import json
import pubmed_parser as pp

# Setting up FTP hook to pubmed ftp server
ftp_hook = FTPHook('pubmed_ftp')
# Setting up MongoDB hook to mlab server
mongodb_hook = MongoHook('mongo_default')


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
    """Deletes directory even if it exists or not

    Args:
        dirname (str): name of directory.
    """
    if os.path.exists(dirname) and os.path.isdir(dirname):
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


def pubmed_get_text(pubmed_paragraph: dict) -> str:
    """Extracts text from pubmed_paragraph

    Args:
        pubmed_paragraph (dist): dict with pubmed_paragraph info
    """
    return " ".join([p["text"] for p in pubmed_paragraph])


def pubmed_get_authors(pubmed_xml: dict) -> list:
    """Extracts authors from pubmed_paragraph

    Args:
        pubmed_xml (dist): dict with pubmed_xml info
    """
    return [" ".join(a[0:-1]) for a in pubmed_xml["author_list"]]

def build_case_report_json(xml_path: str) -> json:
    """Makes and returns a JSON object from pubmed XML files

    Args:
        xml_path (str): path to input XML file
    """
    pubmed_xml = pp.parse_pubmed_xml(xml_path)
    pubmed_paragraph = pp.parse_pubmed_paragraph(xml_path)
    pubmed_references = pp.parse_pubmed_references(xml_path)
    parse_pubmed_table = pp.parse_pubmed_table(xml_path)

    case_report = {
        "pmID": pubmed_xml["pmid"],
        "title": pubmed_xml["full_title"],
        "messages": [],
        "source_files": [],
        "modifications": [],
        "normalizations": [],
        # ctime            : 1351154734.5055847,
        "text": pubmed_get_text(pubmed_paragraph),
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
        "authors": pubmed_get_authors(pubmed_xml),
        "keywords": [],
        "introduction": None,
        "discussion": None,
        "references": []
    }

    return case_report


def extract_pubmed_data() -> None:
    """Extracts case-reports from pubmed data and stores result on local server
    """
    # to test specific tar files
    #pattern = 'non_comm_use.A-B.xml.tar.gz'
    pattern = "*.xml.tar.gz"
    ftp_path = '/pub/pmc/oa_bulk'
    root_dir = '/usr/local/airflow'
    pubmed_dir = os.path.join(root_dir, 'pubmed')
    original_dir = os.path.join(pubmed_dir, 'original')
    prefix = 'case_reports/pubmed/original'

    delete_dir(original_dir)
    create_dir(pubmed_dir)
    create_dir(original_dir)

    filenames = ftp_hook.list_directory(ftp_path)
    filenames = list(
        filter(lambda filename: fnmatch.fnmatch(filename, pattern), filenames))

    for filename in filenames:
        remote_path = os.path.join(ftp_path, filename)
        local_path = os.path.join(original_dir, filename)

        ftp_hook.retrieve_file(remote_path, local_path)
        o_path = extract_original_name(local_path)

        extract_file_case_reports(local_path, o_path)
        delete_file(local_path)
        make_tarfile(local_path, o_path)
        delete_dir(o_path)

def extract_pubmed_data_failure_callback(context) -> None:
    pass


def join_json_data(filenames: str, dest_path: str) -> None:
    """Make a json file consisting of multiple json data

    Args:
        filenames (str): names of input json files
        dest_path (str): directory for combined json output
    """
    outfile = open(dest_path, 'w')
    for filename in filenames:
        new_json = build_case_report_json(filename)
        json.dump(new_json, outfile)
        outfile.write('\n')

    outfile.close()


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
        mongodb_hook.replace_many(collection, docs, filter_docs, upsert=True)


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
