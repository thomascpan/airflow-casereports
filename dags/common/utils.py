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
from elasticsearch import Elasticsearch


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


def get_keywords(subjects: list) -> list:
    """Extracts keywords from subjects if exist

    Args:
        subjects (list): list of subjects

    Returns:
        list: pubmed keywords.
    """
    return subjects[1:] if subjects else []


def get_article_type(subjects: list) -> str:
    """Extracts article_type from subjects if exist

    Args:
        subjects (list): list of subjects

    Returns:
        str: pubmed article type.
    """
    return subjects[0] if subjects else None


def build_case_report_json(xml_path: str) -> dict:
    """Makes and returns a JSON object from pubmed XML files
    Args:
        xml_path (str): path to input XML file
    """
    pubmed_xml = pp.parse_pubmed_xml(xml_path)
    pubmed_paragraph = pp.parse_pubmed_paragraph(xml_path)
    pubmed_references = pp.parse_pubmed_references(xml_path)
    subjects = pubmed_get_subjects(pubmed_xml)
    keywords = get_keywords(subjects)
    article_type = get_article_type(subjects)

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
        "keywords": keywords,
        "introduction": None,
        "discussion": None,
        "references": [],
        "journal": pubmed_xml.get("journal"),
        "article_type": article_type,  # For filtering.
    }

    return case_report


def validate_case_report(case_report: dict) -> bool:
    """Check if case report is valid

    Args:
        case_report (dict): case report dict object
    Returns:
        bool: whether case report is valid
    """
    return case_report.get("pmID")


def text_filter(text: str, terms: list) -> bool:
    """Check if text contains any of the terms
    Args:
        text (str): searched text
        terms (list): list of selected terms
    Returns:
        bool: whether there are any matches
    """
    if isinstance(text, str):
        return any(term.lower() in text.lower() for term in terms)
    return False


def subject_filter(subjects: list, terms: list) -> bool:
    """Check if there are any matching subjects and terms
    Args:
        subjects (list): list of subjects
        terms (list): list of selected terms
    Returns:
        bool: whether there are any matches
    """
    if not subjects or not terms:
        return False
    return any(set(subject.lower() for subject in subjects) & set(term.lower() for term in terms))


def article_type_filter(article_type: str, types: list) -> bool:
    """Check if there is a matching publication type
    Args:
        article_type (str): article_type
        terms (list): list of selected types
    Returns:
        bool: whether there are any matches
    """
    if not article_type or not types:
        return False
    return article_type.lower() in set(t.lower() for t in types)


def case_report_filter(case_report: dict) -> bool:
    """Checks to see if document is a case_report
    Args:
        article_type (str): article_type
        terms (list): list of selected types
    Returns:
        bool: returns whether document is a case_report
    """
    article_type = case_report.get("article_type")
    title = case_report.get("title")
    terms = ["Case Report"]

    atf = article_type_filter(article_type, terms)
    return atf


def generate_elasticsearch_body(es: Elasticsearch, docs: list) -> list:
    """Create elasticserach body for bulk()
    Args:
        es (Elasticsearch): Elasticsearch instance
        docs (list): list of case_report docs
    Returns:
        list: list of bulk update items
    """

    body = []
    for doc in docs:
        if validate_case_report(doc):
            body.append(
                {
                    "_index": "casereport",
                    "_type": "_doc",
                    "_id": doc.get('pmID'),
                    "_source": {
                        "pmID": doc.get('pmID'),
                        "content": doc.get('text')
                    }
                }
            )
    return body


def join_json_data(filenames: str, dest_path: str) -> None:
    """Make a json file consisting of multiple json data

    Args:
        filenames (str): names of input json files
        dest_path (str): directory for combined json output
    """
    outfile = open(dest_path, 'w')

    for filename in filenames:
        new_json = build_case_report_json(filename)

        if validate_case_report(new_json):
            title_terms = ["heart", "cardiology", "heartrhythm",
                           "cardiovascular", "heart rhythm", "cardio", "JACC"]
            text_terms = ["arrhythmia", "heart", "cardiology", "heartrhythm", "cardiovascular", "heart rhythm", "cardio",
                          "angina", "aorta", "arteriography", "arteriosclerosis", "tachycardia", "ischemia", "ventricle", "tricuspid", "valve"]
            title = new_json.get('title')
            text = new_json.get('text')
            journal = new_json.get('journal')
            if case_report_filter(new_json) and (text_filter(journal, title_terms) or text_filter(title, text_terms)):
                del(new_json["article_type"])
                del(new_json["journal"])
                json.dump(new_json, outfile)
                outfile.write('\n')

    outfile.close()

    if os.stat(dest_path).st_size == 0:
        delete_file(dest_path)
