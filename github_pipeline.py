# import dlt
# from dlt.sources.helpers import requests
# import json

# # Specify the URL of the API endpoint
# url = "https://api.github.com/repos/dlt-hub/dlt/issues"

# # Make a request and check if it was successful
# response = requests.get(url)
# response.raise_for_status()

# pipeline = dlt.pipeline(
#     pipeline_name="github_issues",
#     destination="postgres",
#     dataset_name="dltHub",
#     progress="log"
# )
# # The response contains a list of issues

# with open('data.json', 'w') as f:
#     json.dump(response.json(), f)

# load_info = pipeline.run(response.json(), table_name="issues",write_disposition="replace")

# print(load_info)


import dlt
from githubIssues import source
from typing import List


def load(resources: List[str]) -> None:
    """
    Execute a pipeline that will load all the resources for the given endpoints.
    Args:
        resources (List[str]): A list of resource names to load data from the githubIssue source.
    Returns:
        None: This function doesn't return any value. It prints the loading information on successful execution.
    """
    pipeline = dlt.pipeline(
        pipeline_name="githubIssues", destination='postgres', dataset_name="dltHub", progress="log"
    )
    load_info = pipeline.run(source().with_resources(*resources))
    print(load_info)

if __name__ == "__main__":
    """
    Main function to execute the data loading pipeline.
    Add your desired resources to the list and call the load function.
    """
    resources = ["githubIssues"]
    load(resources)