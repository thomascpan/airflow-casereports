def generate_elasticsearch_body(start: int, data: list) -> list:
    """Created elasticserach body for bulk()

    Args:
        start (int): starting index id
        data (list): list of dictionary objects
    Returns:
        list: list of bulk update items
    """
    index_id = start

    body = []

    for d in data:
        body.append(
            {
                "index": {
                    "_index": 'casereport',
                    "_type": '_doc',
                    "_id": index_id
                    "doc_as_upsert": True
                }
            }
        )
        body.append(
            {
                "id": d.casereport_id,
                "pmID": d.pm_id,
                "content": d.text
            }
        )

        index_id += 1

    return body
