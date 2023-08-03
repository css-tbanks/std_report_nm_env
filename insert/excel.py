import excel
import csv
import pandas as pd
import os
from datetime import *
from dotenv import *
load_dotenv()

config = os.environ

runid = datetime.today().strftime("%Y%m%d")

def excel_gen(results):
    path = config['deliverable_path']
    d_path = path + f'/{runid}'

    results.to_csv(d_path, index=False)
    print(f'results have been saved here: {d_path}')