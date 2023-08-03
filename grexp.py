import pandas as pd
from ruamel import yaml
import sys

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

# def start_grexp(path):
context = gx.get_context()

def use_grexp(df, table_name):
    datasource_config = {
        "name": "example_datasource",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "PandasExecutionEngine",
        },
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
                "batch_identifiers": ["default_identifier_name"],
            },
        },
    }

    context.test_yaml_config(yaml.dump(datasource_config))
    context.add_datasource(**datasource_config)


    # path = '//datawarehouse/HIEWarehouseDataStore/NDC_Flow/run_42474_2023-02-23-095812/package_20230223.csv'
    # table_name = 'package'

    # Create a pandas DataFrame with some data
    # df = pd.read_csv(path)

    # turn to string
    df = df.astype(str)

    batch_request = RuntimeBatchRequest(
        datasource_name="example_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="nm_validation",  # This can be anything that identifies this data_asset for you
        runtime_parameters={"batch_data": df},  # df is your dataframe
        batch_identifiers={"default_identifier_name": "default_identifier"},
    )

    context.add_or_update_expectation_suite(expectation_suite_name="test_suite")
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="test_suite"
    )
    print(validator.head())

    if table_name == 'rules_set':
        validator.expect_column_to_exist("benefit_id")
        # validator.expect_column_to_exist("run_id")

        # validator.expect_column_values_to_not_be_null(
        #     column=[],
        #     condition_parser='pandas' # or 'python'
        # )
        
        validator.expect_column_value_lengths_to_equal(column = "FULLNDCCODE", value = 11)


        validator.expect_column_values_to_not_be_null(
            column=["benefitID"],
            condition_parser='pandas' # or 'python'
        )

    result = validator.validate()

    print(f'{table_name} has completed the validation process. Here are the results:')
    print(result)

    if not result["success"]:
        print("Validation failed!")
        sys.exit(1)
    else:
        print(f'Success validation for {table_name}. Proceeding...')
