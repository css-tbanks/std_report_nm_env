import csv
import pandas as pd
import openpyxl
import os
from datetime import *
from dotenv import *
load_dotenv()
import pandas as pd
import re
import openpyxl
from openpyxl.styles import PatternFill

config = os.environ

runid = datetime.today().strftime("%Y%m%d")

# Assuming your DataFrames are named 'df1' and 'df2'

# Perform the merge based on different column names
def benefit_refactor(bill, df):
    results = pd.merge(df, bill, left_on=['benefit_id', 'metal_level', 'variant_id'],
                     right_on=['benefit_id', 'metal', 'plan_id'], how='inner')
    results = num_split(results)
    results = results.rename(columns={'Value': 'Rule Value', 'orig_coin_value': 'SERFF Coinsurance', 'orig_copay_value': 'SERFF Copay'})
    results['carrier_id'] = results['hios_plan_id'].str[:5]
    return results


# def result_func(dductible, df):
#     merged_df = pd.merge(df, dductible, left_on=['benefit_id', 'metal_level', 'variant_id'],
#                      right_on=['benefit_id', 'metal', 'plan_id'], how='left')
#     return merged_df


def num_split(df):
    df['SERFF Copay Value'] = df['orig_copay_value'].str.extract('(\d+)')
    # df['SERFF Copay Value'] = df[df['orig_copay_value' == 'No Charge']]
    # df['SERFF Value'] = df['formatted_value'].replace('[\$,0-9\.]', '', regex=True)
    return df


def bene_match(df):
    df['plan_variant_name'] = df['plan_variant_name'].apply(lambda x: re.sub(r'[^\x00-\x7F]+', '', str(x)))

    # set copay value as float
    df['SERFF Copay Value'] = df['SERFF Copay Value'].astype(float)

    # if no charge, set value = 0
    df.loc[df['SERFF Copay'] == "No Charge", 'SERFF Copay Value'] = 0
    df.loc[df['SERFF Coinsurance'] == "No Charge", 'SERFF Copay Value'] = 0

    # compare rule value with copay value
    df['match_result'] = df['Rule Value'] == df['SERFF Copay Value']
    for _, row in df.iterrows():
        if row['benefit_id'] not in [97,96,73]:
                if row['variant_id'] in [99]:
                    df.loc[df['SERFF Copay'].str.contains('after deductible'), 'match_result'] = False
                    df.loc[df['SERFF Coinsurance'].str.contains('after deductible'), 'match_result'] = False

    return df
