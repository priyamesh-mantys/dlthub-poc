import dlt
from pandaDoc import source
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
        pipeline_name="PDDocumentList", destination='postgres', dataset_name="dltHub", progress="log"
    )
    load_info = pipeline.run(source().with_resources(*resources))
    print(load_info)

if __name__ == "__main__":
    """
    Main function to execute the data loading pipeline.
    Add your desired resources to the list and call the load function.
    """
    resources = ["PDDocumentList"]
    load(resources)