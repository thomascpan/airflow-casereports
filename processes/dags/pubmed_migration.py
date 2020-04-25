from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
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
from pymongo.errors import BulkWriteError

# Setting up boto3 hook to AWS S3
s3_hook = S3Hook('my_conn_S3')
# Setting up MongoDB hook to mlab server
mongodb_hook = MongoHook('mongo_default')
ftp_conn_id = "pubmed_ftp"


def ftp_connect(ftp_conn_id: str) -> FTPHook:
    """Connect to FTP.

    Args:
        ftp_conn_id (str): ftp conn_id.
    Returns:
        FTPHook: FTPHook instance.
    """
    return FTPHook(ftp_conn_id)


def ftp_disconnect(hook: FTPHook) -> None:
    """Disconnect from FTP.

    Args:
        hook (FTPHook): FTPHook instance.
    """
    try:
        hook.close_conn()
    except:
        None


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


def pubmed_get_text(pubmed_paragraph: list) -> str:
    """Extracts text from pubmed_paragraph

    Args:
        pubmed_paragraph (list): list with pubmed_paragraph info

    Returns:
        str: pubmed text body.
    """
    result = " ".join([p.get("text") for p in pubmed_paragraph])
    return result or None


def get_author(author: list) -> str:
    """Converts author in list format to string.

    Args:
        author (list): author in list format (['last_name_1', 'first_name_1', 'aff_key_1'])

    Returns:
        str: The original name
    """
    first_name = author[1] or ""
    last_name = author[0] or ""
    return ("%s %s" % (first_name, last_name)).strip()


def pubmed_get_authors(pubmed_xml: dict) -> list:
    """Extracts authors from pubmed_xml

    Args:
        pubmed_xml (dist): dict with pubmed_xml info

    Returns:
        list: pubmed authors.
    """
    author_list = pubmed_xml.get("author_list")
    result = None
    if author_list:
        result = [get_author(a) for a in author_list]
    return result


def pubmed_get_subjects(pubmed_xml: dict) -> list:
    """Extracts subjects from pubmed_xml.
    List of subjects listed in the article.
    Sometimes, it only contains type of article, such as research article,
    review proceedings, etc

    Args:
        pubmed_xml (dist): dict with pubmed_xml info

    Returns:
        list: pubmed subjects.
    """
    return list(filter(None, map(lambda x: x.strip(), pubmed_xml.get("subjects").split(";"))))


def build_case_report_json(xml_path: str) -> json:
    """Makes and returns a JSON object from pubmed XML files
    Args:
        xml_path (str): path to input XML file
    """
    pubmed_xml = pp.parse_pubmed_xml(xml_path)
    pubmed_paragraph = pp.parse_pubmed_paragraph(xml_path)
    pubmed_references = pp.parse_pubmed_references(xml_path)

    case_report = {
        "pmID": pubmed_xml.get("pmid"),
        "doi": pubmed_xml.get("doi"),
        "title": pubmed_xml.get("full_title"),
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
        "abstract": pubmed_xml.get("abstract"),
        "authors": pubmed_get_authors(pubmed_xml),
        "keywords": pubmed_get_subjects(pubmed_xml),
        "introduction": None,
        "discussion": None,
        "references": []
    }

    return case_report


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

        collection = 'caseReports'
        filter_docs = [{'pmID': doc['pmID']} for doc in docs]

        try:
            mongodb_hook.replace_many(
                collection, docs, filter_docs, upsert=True)
        except BulkWriteError as bwe:
            logging.info(bwe.details)
            logging.info(bwe.details['writeErrors'])
            raise bwe


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

extract_pubmed_data_task >> transform_pubmed_data_task >> update_mongodb_task
