from book import Book, get_logger
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
import pandas as pd
import json

logger = get_logger(__name__)

def book_to_dataframe(book: Book) -> pd.DataFrame:
    """
    Converts a Book object to a pandas DataFrame.
    This is a simplified conversion and might need to be adapted for complex hierarchies.
    """
    all_nodes = []
    def get_all_nodes(nodes):
        for node in nodes:
            all_nodes.append(node.properties)
            get_all_nodes(node.children)
    
    get_all_nodes(book.root_nodes)
    
    return pd.DataFrame(all_nodes)


def generate_expectations_from_book(book: Book, suite_name: str) -> str:
    """
    Generates a Great Expectations Expectation Suite from a Book object.
    Returns the path to the generated suite file.
    """
    logger.info(f"Generating Expectation Suite for Book: {book.name}...")

    # Convert Book to DataFrame
    df = book_to_dataframe(book)

    # Get a Data Context
    context = gx.get_context(project_root_dir='great_expectations')


    # Define a Datasource and Data Asset
    datasource_name = f"{book.name.lower().replace(' ', '_')}_datasource"
    data_asset_name = f"{book.name.lower().replace(' ', '_')}_asset"
    
    datasource = context.sources.add_pandas(name=datasource_name)
    data_asset = datasource.add_dataframe_asset(name=data_asset_name)
    batch_request = data_asset.build_batch_request(dataframe=df)

    # Create an Expectation Suite
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    # Add some basic expectations
    for column in df.columns:
        validator.expect_column_to_exist(column)
        validator.expect_column_values_to_not_be_null(column, mostly=0.8) # Allow some nulls

    # Save the suite
    validator.save_expectation_suite(discard_failed_expectations=False)

    # Get the path to the suite
    suite = context.get_expectation_suite(expectation_suite_name=suite_name)
    suite_path = context.stores.expectations_store.get_key(suite.ge_cloud_id, suite.name).to_string()
    suite_path = f"great_expectations/expectations/{suite_name}.json"
    
    logger.info(f"Successfully generated Expectation Suite at: {suite_path}")

    return suite_path

