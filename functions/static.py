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
def refactor(df2, df, filetype):
    df['benefit_id'] = df['benefit_id'].apply(lambda x: 18 if x == 65 else x)
    results = pd.merge(df, df2, left_on=['benefit_id', 'metal_level', 'variant_id'],
                     right_on=['benefit_id', 'metal', 'plan_id'], how='inner')
    results = results.rename(columns={'Value': 'Rule Value', 'formatted_value': f'SERFF {filetype}'})
    results['carrier_id'] = results['cost_sharing_variant'].str[:5]
    num_split(results,filetype)
    return results


def match_results(df, filetype):
    df['plan_variant_name'] = df['plan_variant_name'].apply(lambda x: re.sub(r'[^\x00-\x7F]+', '', str(x)))

    df[f'SERFF {filetype} Value'] = df[f'SERFF {filetype} Value'].astype(float)

    # compare rule value with copay value
    df['match_result'] = df['Rule Value'] == df[f'SERFF {filetype} Value']

    return df

def num_split(df, filetype):
    df[f'SERFF {filetype} Value'] = df[f'SERFF {filetype}'].str.replace(',', '')
    df[f'SERFF {filetype} Value'] = df[f'SERFF {filetype} Value'].str.extract('(\d+)')
    # df['SERFF Copay Value'] = df[df['orig_copay_value' == 'No Charge']]
    # df['SERFF Value'] = df['formatted_value'].replace('[\$,0-9\.]', '', regex=True)
    return df

# import pandas as pd

# def duct_excel(results):
#     # select only selected columns
#     selected_columns = ['benefit_name', 'variant_id', 'plan_variant_name', 'cost_sharing_variant', 'SERFF Deductible Value', 'Rule Value', 'match_result']

#     # generate df with only selected columns
#     df = results[selected_columns].copy()
#     path = config['deliverable_path']
#     file_path = path + f'nm_serff_comp_{runid}.xlsx'  # Use the same filename

#     sheet_name = 'Deductible'
#     # connect to file
#     writer = pd.ExcelWriter(file_path, engine='xlsxwriter')

#     # Check if the sheet already exists
#     sheet_index = 2
#     new_sheet_name = sheet_name
#     while new_sheet_name in writer.sheets:
#         new_sheet_name = f'{sheet_name}_{sheet_index}'
#         sheet_index += 1

#     df.to_excel(writer, sheet_name=new_sheet_name, index=False)
#     workbook = writer.book

#     # assign sheet
#     worksheet = writer.sheets[new_sheet_name]
#     worksheet.autofilter(0, 0, df.shape[0], df.shape[1] - 1)

#     # save file
#     writer.save()
#     print(f'results have been saved here: {file_path}')
#     return df

