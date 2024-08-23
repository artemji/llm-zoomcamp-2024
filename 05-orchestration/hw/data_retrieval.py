from typing import Dict, List
from elasticsearch import Elasticsearch


@data_loader
def search(*args, **kwargs) -> List[Dict]:
    """
    Executes a search query on an Elasticsearch index.

    Args:
        connection_string (str): Elasticsearch connection URL.
        index_name (str): Name of the index to search.
        *args: Additional arguments.
        **kwargs: Additional keyword arguments.

    Returns:
        List[Dict]: A list of documents matching the search query.
    """
    connection_string = kwargs.get(
        'connection_string', 'http://localhost:9200')
    index_name = kwargs.get('index_name', 'documents')

    # Define the search query to be executed
    script_query = {
        "size": 5,
        "query": {
            "bool": {
                "must": {
                    "multi_match": {
                        "query": "When is the next cohort?",
                        "fields": ["question^4", "text^2", "section"],
                        "type": "best_fields"
                    }
                }
            }
        }
    }

    es_client = Elasticsearch(connection_string)

    try:
        # Execute the search query
        response = es_client.search(
            index=index_name,
            body=script_query,
        )
        # Return the source of each hit in the search results
        return [hit['_source'] for hit in response['hits']['hits']]
    except Exception as e:
        print(f"Error executing search: {e}")
        return []
