import pandas as pd
from ruamel import yaml
import sys

import pandas as pd
from ruamel import yaml
import sys

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

from false_handling import *
from functions.main import *

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
    # print(validator.head())
    validator.expect_column_values_to_not_be_null(
        column=["benefit_name", "variant_id", "carrier_id", "plan_variant_name", "match_result"],
        condition_parser='pandas' # or 'python'
    )

    if table_name == 'bill':

        
        validator.expect_column_to_exist("benefit_name")
        validator.expect_column_to_exist("variant_id")
        validator.expect_column_to_exist("carrier_id")
        validator.expect_column_to_exist("plan_variant_name")
        validator.expect_column_to_exist("hios_plan_id")
        validator.expect_column_to_exist("SERFF Coinsurance")
        validator.expect_column_to_exist("SERFF Copay")
        validator.expect_column_to_exist("SERFF Copay Value")
        validator.expect_column_to_exist("Rule Value")
        validator.expect_column_to_exist("match_result")
        validator.expect_column_to_exist("Result Description")

        df = bene_result_description(df)
        df = shrink_df(df, 'benefits')



       
    elif table_name == 'duct':


        validator.expect_column_to_exist("benefit_name")
        validator.expect_column_to_exist("variant_id")
        validator.expect_column_to_exist("carrier_id")
        validator.expect_column_to_exist("plan_variant_name")
        validator.expect_column_to_exist("cost_sharing_variant")
        validator.expect_column_to_exist("SERFF Deductible")
        validator.expect_column_to_exist("SERFF Deductible Value")	
        validator.expect_column_to_exist("Rule Value")
        validator.expect_column_to_exist("match_result")
        validator.expect_column_to_exist("Result Description")

        #-----------------------------------

        df = static_result_description(df, 'Deductible')
        df = shrink_df(df, 'Deductible')
       
    elif table_name == 'moop':


        validator.expect_column_to_exist("benefit_name")
        validator.expect_column_to_exist("variant_id")
        validator.expect_column_to_exist("carrier_id")
        validator.expect_column_to_exist("plan_variant_name")
        validator.expect_column_to_exist("cost_sharing_variant")
        validator.expect_column_to_exist("SERFF MOOP")
        validator.expect_column_to_exist("SERFF MOOP Value")	
        validator.expect_column_to_exist("Rule Value")
        validator.expect_column_to_exist("match_result")
        validator.expect_column_to_exist("Result Description")

        #-----------------------------------

        df = static_result_description(df, 'MOOP')
        df = shrink_df(df, 'MOOP')

    result = validator.validate()

    # print(f'{table_name} has completed the validation process. Here are the results:')

    if not result["success"]:
        print("Validation failed!")
        result = False
    else:
        print(f'Success validation for {table_name}. Proceeding...')
        result = True
    

    return df