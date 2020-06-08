import time
import os
import tarfile
import shutil


def extract_original_name(filepath: str) -> str:
    """Extracts original name from tar.gz file
    Args:
        filepath (str): path of file.
    Returns:
        str: The original name
    """
    name = filepath.split(".")
    return ".".join(name[0:-2])


def delete_file(filepath: str) -> None:
    """Delete file.
    Args:
        filepath (str): path of file.
    """
    if os.path.exists(filepath):
        os.remove(filepath)


def delete_dir(dirname: str) -> None:
    """Deletes directory even if exist
    Args:
        dirname (str): name of directory.
    """
    shutil.rmtree(dirname)


def batch():
    filter_pattern = "Case_Rep"
    filepath = '/Users/thomaspan/Downloads/non_comm_use.A-B.xml.tar.gz'
    output_filepath = '/Users/thomaspan/Downloads/bout_non_comm_use.A-B.xml.tar.gz'
    dest_dir = extract_original_name(filepath)
    source_dir = dest_dir
    with tarfile.open(filepath) as tf:
        for entry in tf:
            if filter_pattern:
                if filter_pattern in entry.name:
                    tf.extract(entry, dest_dir)
            else:
                tf.extract(entry, dest_dir)

    with tarfile.open(output_filepath, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

    delete_dir(dest_dir)


def stream():
    filter_pattern = "Case_Rep"
    filepath = '/Users/thomaspan/Downloads/non_comm_use.A-B.xml.tar.gz'
    output_filepath = '/Users/thomaspan/Downloads/sout_non_comm_use.A-B.xml.tar.gz'
    dest_dir = extract_original_name(filepath)
    source_dir = dest_dir
    with tarfile.open(filepath, 'r|gz') as rtf:
        with tarfile.open(output_filepath, "w|gz") as wtf:
            for entry in rtf:
                if filter_pattern:
                    if filter_pattern in entry.name:
                        fileobj = rtf.extractfile(entry)
                        wtf.addfile(entry, fileobj)
                else:
                        fileobj = rtf.extractfile(entry)
                        wtf.addfile(entry, fileobj)


def main():

    start = time.time()
    batch()
    end = time.time()
    print(end - start)

    start = time.time()
    stream()
    end = time.time()
    print(end - start)


if __name__ == "__main__":
    main()
