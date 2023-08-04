import pandas as pd
from ruamel import yaml
import sys

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
        data_asset_name="testing_ndc",  # This can be anything that identifies this data_asset for you
        runtime_parameters={"batch_data": df},  # df is your dataframe
        batch_identifiers={"default_identifier_name": "default_identifier"},
    )

    context.add_or_update_expectation_suite(expectation_suite_name="test_suite")
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="test_suite"
    )
    print(validator.head())

    if table_name == 'package':
        validator.expect_column_to_exist("PRODUCTID")
        validator.expect_column_to_exist("PRODUCTNDC")
        validator.expect_column_to_exist("NDCPACKAGECODE")
        validator.expect_column_to_exist("PACKAGEDESCRIPTION")
        validator.expect_column_to_exist("STARTMARKETINGDATE")
        validator.expect_column_to_exist("ENDMARKETINGDATE")
        validator.expect_column_to_exist("NDC_EXCLUDE_FLAG")
        validator.expect_column_to_exist("SAMPLE_PACKAGE")
        # validator.expect_column_to_exist("run_id")

        validator.expect_column_values_to_not_be_null(
            column=["PRODUCTID", "PRODUCTNDC", "PACKAGEDESCRIPTION"],
            condition_parser='pandas' # or 'python'
        )

    elif table_name == 'product':
        validator.expect_column_to_exist("PRODUCTID")
        validator.expect_column_to_exist("PRODUCTNDC")
        validator.expect_column_to_exist("PRODUCTTYPENAME")
        validator.expect_column_to_exist("PROPRIETARYNAME")
        validator.expect_column_to_exist("PROPRIETARYNAMESUFFIX")
        validator.expect_column_to_exist("NONPROPRIETARYNAME")
        validator.expect_column_to_exist("DOSAGEFORMNAME")
        validator.expect_column_to_exist("ROUTENAME")
        validator.expect_column_to_exist("STARTMARKETINGDATE")
        validator.expect_column_to_exist("ENDMARKETINGDATE")
        validator.expect_column_to_exist("MARKETINGCATEGORYNAME")
        validator.expect_column_to_exist("APPLICATIONNUMBER")
        validator.expect_column_to_exist("LABELERNAME")
        validator.expect_column_to_exist("SUBSTANCENAME")
        validator.expect_column_to_exist("ACTIVE_NUMERATOR_STRENGTH")
        validator.expect_column_to_exist("ACTIVE_INGRED_UNIT")
        validator.expect_column_to_exist("PHARM_CLASSES")
        validator.expect_column_to_exist("DEASCHEDULE")
        validator.expect_column_to_exist("NDC_EXCLUDE_FLAG")
        validator.expect_column_to_exist("LISTING_RECORD_CERTIFIED_THROUGH")
        # validator.expect_column_to_exist("run_id")

        validator.expect_column_values_to_not_be_null(
            column=["PRODUCTID", "PRODUCTNDC", "PRODUCTTYPENAME", "PROPRIETARYNAME"],
            condition_parser='pandas' # or 'python'
        )

    elif table_name == 'temp_ndc':
        validator.expect_column_value_lengths_to_equal(column = "fullndccode", value = 11)
        
        validator.expect_column_to_exist("runid")
        validator.expect_column_to_exist("productid")
        validator.expect_column_to_exist("productndc")
        validator.expect_column_to_exist("ndcpackagecode")
        validator.expect_column_to_exist("packagedescription")
        validator.expect_column_to_exist("packagemarketingstart")
        validator.expect_column_to_exist("packagemarketingend")
        validator.expect_column_to_exist("packageexcludeflag")
        validator.expect_column_to_exist("samplepackage")
        validator.expect_column_to_exist("producttypename")
        validator.expect_column_to_exist("proprietaryname")
        validator.expect_column_to_exist("proprietarynamesuffix")
        validator.expect_column_to_exist("nonproprietaryname")
        validator.expect_column_to_exist("dosageformname")
        validator.expect_column_to_exist("routename")
        validator.expect_column_to_exist("productmarketingstart")
        validator.expect_column_to_exist("productmarketingend")
        validator.expect_column_to_exist("marketingcategoryname")
        validator.expect_column_to_exist("applicationnumber")
        validator.expect_column_to_exist("labelername")
        validator.expect_column_to_exist("substancename")
        validator.expect_column_to_exist("activenumeratorstrength")
        validator.expect_column_to_exist("activeingredunit")
        validator.expect_column_to_exist("pharmclasses")
        validator.expect_column_to_exist("deaschedule")
        validator.expect_column_to_exist("productexcludeflag")
        validator.expect_column_to_exist("listingrecordcertifiedthrough")
        validator.expect_column_to_exist("fullndccode")
        # validator.expect_column_to_exist("run_id")

        validator.expect_column_values_to_not_be_null(
            column=["productid", "productndc", "packagedescription", "fullndccode"],
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
