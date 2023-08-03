import pandas as pd
import csv


def bene_result_description(df):
    result_description = []
    for _, row in df.iterrows():
        if row['match_result'] == False:
            if row['variant_id'] not in [99]:
                if row['benefit_id'] not in [97,96,73]:
                    if row['Rule Value'] != row['SERFF Copay Value']:
                        result_description.append('SERFF value does not match rule')
                    elif 'after deductible' in row['SERFF Copay']:
                        result_description.append("Value should not include 'after deductible'")
                    elif 'with deductible' in row['SERFF Copay']:
                        result_description.append("Value should not include 'with deductible'")
                    elif pd.isnull(row['SERFF Copay Value']):
                        result_description.append('SERFF Copay has no value')
                    else:
                        result_description.append('')
                else:
                    result_description.append('')
            else:
                result_description.append('')
        else:
            result_description.append('')
    df['Result Description'] = result_description
    return df

def static_result_description(df, name):
    result_description = []
    for _, row in df.iterrows():
        if row['match_result'] == False:
            if row['Rule Value'] != row[f'SERFF_{name}_Value']:
                result_description.append('SERFF value does not match rule')
            elif pd.isnull(row[f'SERFF_{name}_Value']):
                result_description.append('SERFF value is empty')
            else:
                result_description.append('')
        else:
            result_description.append('')
    df['Result Description'] = result_description
    return df

