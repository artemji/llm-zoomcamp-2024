import re
import hashlib
from typing import Any, Dict, List


def generate_document_id(doc: Dict[str, Any]) -> str:
    """
    Generates a unique document ID based on the course, question, and text content.

    Args:
        doc (Dict[str, Any]): A dictionary representing a document.

    Returns:
        str: An 8-character hash that serves as the document ID.
    """
    # Create a string that combines the course, question, and first 10 characters of the text
    combined = f"{doc['course']}-{doc['question']}-{doc['text'][:10]}"

    # Generate an MD5 hash from the combined string
    hash_object = hashlib.md5(combined.encode())
    hash_hex = hash_object.hexdigest()

    # Use the first 8 characters of the hash as the document ID
    return hash_hex[:8]


@transformer
def chunk_documents(data: List[Dict[str, Any]], *args, **kwargs) -> List[Dict[str, Any]]:
    """
    Processes a list of documents, generates unique IDs for each, and returns the processed documents.

    Args:
        data (List[Dict[str, Any]]): A list containing dictionaries with document data.
        *args: Additional positional arguments.
        **kwargs: Additional keyword arguments.

    Returns:
        List[Dict[str, Any]]: A list of documents with added 'document_id' field.
    """
    documents = []

    # Extract the course information from the first item in the data list
    course = data[0]['course']

    for doc in data[0]['documents']:
        # Assign the course name to each document
        doc['course'] = course

        # Generate and assign a unique document ID
        doc['document_id'] = generate_document_id(doc)

        # Append the processed document to the documents list
        documents.append(doc)

    # Output the number of processed documents
    print(f"Processed {len(documents)} documents.")

    return documents
