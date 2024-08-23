import json
from typing import Dict, List, Tuple, Union
import numpy as np
from elasticsearch import Elasticsearch
from datetime import datetime
from mage_ai.data_preparation.variable_manager import set_global_variable


@data_exporter
def elasticsearch(documents: List[Dict[str, Union[str, int, float, None]]], *args, **kwargs) -> None:
    """
    Exports a list of documents to an Elasticsearch index.

    Args:
        documents (List[Dict[str, Union[str, int, float, None]]]): List of documents to index.
        *args: Additional positional arguments.
        **kwargs: Additional keyword arguments.

    Keyword Args:
        connection_string (str): Elasticsearch connection URL. Default is 'http://localhost:9200'.
        index_name (str): Prefix for the index name. Default is 'documents'.
        number_of_shards (int): Number of shards for the index. Default is 1.
        number_of_replicas (int): Number of replicas for the index. Default is 0.
        dimensions (Optional): Additional dimensions or settings.

    Returns:
        None
    """
    # Get connection details and index naming
    connection_string = kwargs.get(
        'connection_string', 'http://localhost:9200')
    index_name_prefix = kwargs.get('index_name', 'documents')
    # Updated format to include hours and seconds
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    index_name = f"{index_name_prefix}_{current_time}"
    print(f"Index name: {index_name}")

    # Retrieve index settings
    number_of_shards = kwargs.get('number_of_shards', 1)
    number_of_replicas = kwargs.get('number_of_replicas', 0)
    dimensions = kwargs.get('dimensions')

    # Store the index name in a global variable for future use
    set_global_variable('ominous_nebula', 'index_name', index_name)

    # Initialize Elasticsearch client
    es_client = Elasticsearch(connection_string)
    print(f'Connecting to Elasticsearch at {connection_string}')

    # Define index settings and mappings
    index_settings = {
        "settings": {
            "number_of_shards": number_of_shards,
            "number_of_replicas": number_of_replicas
        },
        "mappings": {
            "properties": {
                "text": {"type": "text"},
                "section": {"type": "text"},
                "question": {"type": "text"},
                "course": {"type": "keyword"},
                "document_id": {"type": "keyword"}
            }
        }
    }

    # Recreate the index by deleting it if it exists, then create it with the new settings
    if es_client.indices.exists(index=index_name):
        es_client.indices.delete(index=index_name)
        print(f'Index {index_name} deleted')

    es_client.indices.create(index=index_name, body=index_settings)

    # Index each document
    for document in documents:
        es_client.index(index=index_name, document=document)

    # Print the last indexed document for verification
    print(f"Last indexed document: {json.dumps(document, indent=2)}")
