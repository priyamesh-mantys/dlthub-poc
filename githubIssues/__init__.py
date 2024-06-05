"""
This source provides data extraction from an example source as a starting point for new pipelines.
Available resources: [berries, pokemon]
"""

import typing as t
from typing import Sequence, Iterable, Dict, Any
import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource
from dlt.sources.helpers import requests
from .settings import GITHUB_ISSUE_URL
import json


@dlt.resource(table_name="github_issues", write_disposition="replace",)
def githubIssues() -> Iterable[TDataItem]:
    """
    Returns a list of githubIssues.
    Yields:
        dict: The githubIssues data.
    """

    response = requests.get(GITHUB_ISSUE_URL)

    with open('data.json', 'w') as f:
        json.dump(response.json(), f)

    yield response.json()




@dlt.source
def source() -> Sequence[DltResource]:
    """
    The source function that returns all availble resources.
    Returns:
        Sequence[DltResource]: A sequence of DltResource objects containing the fetched data.
    """
    return [githubIssues]
