from sql.alchemy_engine import *
from sqlalchemy import text
from dotenv import *
import pandas as pd
import os
import pyodbc
load_dotenv()

config = os.environ

def rule_import(df):
    # use for sql import configuration
    table = config['import.table']
    schema_name = config['import.schema']
    df.to_sql(
    name=table,
    con=import_engine,
    schema=schema_name,
    chunksize = 1000,
    if_exists='replace',
    index=False)


def bill_extract():
    # pull billable query from env
    query = config['extract.billable']
    # connect to extract engine
    conn = extract_engine.connect()
    results = conn.execute(text(query))
    rows = results.fetchall()
    column_names = results.keys()
    conn.close()
    df = pd.DataFrame(rows, columns=column_names)
    return df


def duct_extract():
    # pull dductible query from env
    query = config['extract.deductible']
    # connect to extract engine
    conn = extract_engine.connect()
    results = conn.execute(text(query))
    rows = results.fetchall()
    column_names = results.keys()
    # close connection
    conn.close()
    results = pd.DataFrame(rows, columns=column_names)
    return results

def moop_extract():
    # pull dductible query from env
    query = config['extract.moop']
    # connect to extract engine
    conn = extract_engine.connect()
    results = conn.execute(text(query))
    rows = results.fetchall()
    column_names = results.keys()
    # close connection
    conn.close()
    results = pd.DataFrame(rows, columns=column_names)
    return results

print('imported sqlalchemy.text')
