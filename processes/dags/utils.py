import os
import logging
import tarfile
import ntpath
import shutil
import fnmatch
import glob
import json
import pubmed_parser as pp
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.contrib.hooks.mongo_hook import MongoHook

ftp_conn_id = "pubmed_ftp"
# Setting up MongoDB hook to mlab server
mongodb_hook = MongoHook('mongo_default')


def mongo_insert(collection: str, docs: list, filter_docs: list) -> None:
        """Updates mongoDB and replaces if entry already exists.

        Args:
            collection: name of mongoDB collection
            docs: documents to update with
            filter_docs: key to filter documents
        """
        mongodb_hook.replace_many(collection, docs, filter_docs, upsert=True)



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
    """Extracts authors from pubmed_paragraph

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
    s_list = list(filter(None, map(lambda x: x.strip(), pubmed_xml.get("subjects").split(";"))))
    return s_list[1:]

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
        "pmID": pubmed_xml.get("pmid"),
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
