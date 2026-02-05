from book.models import Book
from book.logger import get_logger
import pandas as pd
from book.great_expectations_integration.generator import book_to_dataframe

logger = get_logger(__name__)

def validate_book_with_expectations(book: Book, suite_path: str) -> bool:
    """
    Validates a Book object against a Great Expectations Expectation Suite.
    Attaches the validation results to the Book's nodes.
    Returns True if the validation succeeds, False otherwise.
    """
    import great_expectations as gx
    logger.info(f"Validating Book: {book.name} with suite: {suite_path}...")

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

    # Run validation
    checkpoint_name = f"{book.name.lower().replace(' ', '_')}_checkpoint"
    checkpoint_config = {
        "name": checkpoint_name,
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "run_name_template": "%Y%m%d-%H%M%S-my-run",
        "validations": [
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite_path,
            }
        ],
    }
    context.add_or_update_checkpoint(**checkpoint_config)
    checkpoint_result = context.run_checkpoint(checkpoint_name=checkpoint_name)
    
    # Attach results to the book
    validation_result = checkpoint_result.list_validation_results()[0]
    for node in book.root_nodes:
        # This is a simplification. A more robust implementation would map
        # validation results to the specific nodes that failed.
        node.properties["validation_results"] = validation_result.to_json_dict()

    if checkpoint_result.success:
        logger.info("Validation successful!")
    else:
        logger.warning("Validation failed.")

    return checkpoint_result.success
