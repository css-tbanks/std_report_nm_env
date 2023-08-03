import os
import csv
import chardet
from datetime import *
import pandas as pd
import pandas as pd
import numpy as np

from dotenv import *
from sql.alchemy_engine import *

load_dotenv()

config = os.environ

# use environment to configure static variables

# use for file/path configuration
path = config['fpath']
file = config['file']

# generate dataframe using pandas
def df_gen(path, file):
    file_path = path + '/' + file
    df = pd.read_excel(file_path)
    return df

# get rid of carriage returns, turn nans into null for sql, etc.
def quick_clean(df):
    df = df.replace(r'^\s*$', pd.np.nan, regex=True)
    df = df.replace('\r', '', regex=True)
    return df

def meta_gen(df):
    df['date_created'] = datetime.now()
    df['run_id'] = datetime.today().strftime("%Y%m%d")
    df['user_name'] = config['user']
    return df

def data_assign(df):
    # Define the column names and their desired positions
    new_columns = ['variant_id', 'metal_level']
    positions = [3, 4]  # Insert 'metal_level' at position 3 and 'variant_id' at position 4

    # Insert the new columns at the specified positions
    for col, pos in zip(new_columns, positions):
        df.insert(pos, col, '')

    # Print the updated DataFrame
    print(df)
    # Define the conditions and the corresponding values
    conditions = [
        df['plan_name'] == 'Turquoise 1',
        df['plan_name'] == 'Turquoise 2',
        df['plan_name'] == 'Turquoise 3',
        df['plan_name'] == 'Gold 80',
        df['plan_name'] == 'Silver 70',
        df['plan_name'] == 'Silver 94',
        df['plan_name'] == 'Silver 87',
        df['plan_name'] == 'Silver 73'
    ]
    var_values = ['99', '95', '90', '01', '01', '06', '05', '04']
    metal_values = ['Silver','Silver','Gold','Gold','Silver','Silver','Silver','Silver']

    # Use numpy.select to assign values based on conditions
    df['variant_id'] = np.select(conditions, var_values, default=None)
    df['metal_level'] = np.select(conditions, metal_values, default=None)

    return df


df = df_gen(path, file)
df = quick_clean(df)
df = meta_gen(df)
df = data_assign(df)
df['file_id'] = 'rule_template'
# rule_import(df)

# print('done')
# def df_rotation(df):
#     df = df.transpose()  # Transpose the DataFrame

#     new_header = df.iloc[0]  # Save the first row as the new column headers
#     df = df[1:]  # Remove the first row from the DataFrame
#     df.columns = new_header  # Assign the new column headers

#     df.reset_index(drop=True, inplace=True)
#     return df
# df = df_rotation(df)