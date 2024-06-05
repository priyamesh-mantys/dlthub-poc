import typing as t
from typing import Sequence, Iterable
import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource
from dlt.sources.helpers import requests
from .settings import PANDADOC_DOCUMENT_LIST_URL
from .helpers import get_auth_headers
import json


@dlt.resource(table_name="PD_document_list", write_disposition="replace")
def PDDocumentList(api_secret_key=dlt.secrets.value) -> Iterable[TDataItem]:
    """
    Returns a list of githubIssues.
    Yields:
        dict: The githubIssues data.
    """

    print("api_secret_key",api_secret_key)
    headers = get_auth_headers(api_secret_key)
    response = requests.get(PANDADOC_DOCUMENT_LIST_URL, headers=headers)

    print("Document listtttttt",response.json())

    yield response.json()




@dlt.source
def source() -> Sequence[DltResource]:
    """
    The source function that returns all availble resources.
    Returns:
        Sequence[DltResource]: A sequence of DltResource objects containing the fetched data.
    """
    return [PDDocumentList]
