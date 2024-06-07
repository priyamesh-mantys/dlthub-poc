import typing as t
from typing import Sequence, Iterable
import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource
from dlt.sources.helpers import requests
from .settings import PANDADOC_DOCUMENT_LIST_URL
from .helpers import get_auth_headers
from dlt.sources.helpers.rest_client import paginate
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()


# @dlt.resource(table_name="PD_document_list", write_disposition="append")
# def PDDocumentList(api_secret_key=dlt.secrets.value) -> Iterable[TDataItem]:
#     """
#     Returns a list of githubIssues.
#     Yields:
#         dict: The githubIssues data.
#     """

#     print("api_secret_key",api_secret_key)
#     headers = get_auth_headers(api_secret_key)
#     response = requests.get(PANDADOC_DOCUMENT_LIST_URL, headers=headers)

#     print("Document listtttttt",response.json())

#     yield response.json()


# @dlt.resource(
#     table_name="PD_document_list",
# )
# def PDDocumentList(
#     date_modified = dlt.sources.incremental("date_created", initial_value="1970-01-01T00:00:00Z"),
#     api_secret_key=dlt.secrets.value
# ):
#     headers = get_auth_headers(api_secret_key)
#     for page in paginate(
#         PANDADOC_DOCUMENT_LIST_URL,
#         headers=headers,
#         data_selector="results",
#         params={
#             "created_from": date_modified.last_value,
#             "status":2
#         }
#     ):
#         print("page", page)
#         yield page


# @dlt.resource(
#     table_name="PD_document_list",
#     write_disposition="merge",
#     primary_key="id"
# )
# def PDDocumentList(api_secret_key=dlt.secrets.value, 
#                    date_modified = dlt.sources.incremental("date_created", initial_value="2024-04-01T00:00:00.000000Z")):
#     headers = get_auth_headers(api_secret_key)
    
#     count = 1
#     flag = True
#     while flag:
#         params={
#             "status":2,
#             "created_from": date_modified.last_value,
#             "count":100,
#             "order_by":'date_modified',
#         }
#         response = requests.get(PANDADOC_DOCUMENT_LIST_URL, headers=headers, params=params)
#         response.raise_for_status()
#         data = response.json()
#         results = data.get("results", [])


#         print("result", len(results))
#         print("count", count)
#         print("date_modified", date_modified.last_value)

#         if not results or count==5:
#             flag=False 
#         count += 1

#         yield results


@dlt.resource(
    table_name="PD_document_list",
    write_disposition="merge",
    primary_key="id"
)
def PDDocumentList(api_secret_key=dlt.secrets.value):
    created_from = '2024-04-01T00:00:00.000000Z'
    # created_to = '2024-05-01T00:00:00.000000Z'

    try:

        conn = psycopg2.connect(database = "domain_client", 
                            user = "postgres", 
                            host= os.environ.get('POSTGRES_HOST'),
                            password = os.environ.get('POSTGRES_PASSWORD'),
                            port = 5432)
        
        #Setting auto commit false
        conn.autocommit = True
        #Creating a cursor object using the cursor() method
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        #Retrieving data
        cursor.execute('''SELECT * FROM dlt_hub.pd_document_list order by date_created DESC limit 1''')
        #Fetching 1st row from the table
        result = cursor.fetchone()
        # print("first row" ,result)
        # print("date_created db",result['date_created'])
        # created_from = result['date_created']
        #Commit your changes in the database
        conn.commit()
        #Closing the connection
        conn.close()
    except Exception as e:
        pass
    
    headers = get_auth_headers(api_secret_key)
    
    page = 1
    flag = True
    while flag:
        params={
            "status":2,
            "created_from": created_from,
            "count":100,
            "order_by":'date_created',
            "page":page,
            # "created_to": created_to
        }
        response = requests.get(PANDADOC_DOCUMENT_LIST_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])

        for result in results:
            result["status"] = "EXTRACTED_ID"
            result["extracted_contract_json"] = None
            result["extracted_contract_time"] = None
            result["source"] = 'pandadoc'
            result["document_link"] = ""
        

        print("page", page)
        print("result", len(results))

        if not results:
            flag=False 
        page += 1

        yield results
    

@dlt.source
def source() -> Sequence[DltResource]:
    """
    The source function that returns all availble resources.
    Returns:
        Sequence[DltResource]: A sequence of DltResource objects containing the fetched data.
    """
    return [PDDocumentList]
