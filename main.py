from sql.alchemy_engine import *
from sql.main import *
from rule.rule_table_prep import *
from functions.benefit import *
from functions.main import *
from functions.static import *
from false_handling import *


def bill_master():
    bill = bill_extract()
    results = benefit_refactor(bill,df)
    results = bene_match(results)
    # finished_bill = bene_excel(results)
    # recolor_sheets('Benefits')
    results = bene_result_description(results)
    results = shrink_df(results, 'benefits')
    return results

def duct_master():
    duct = duct_extract()
    results = refactor(duct,df,'Deductible')
    results = match_results(results,'Deductible')
    # finished_duct = duct_excel(results)
    # recolor_sheets('Deductible')
    results = static_result_description(results, 'Deductible')
    results = shrink_df(results, 'Deductible')
    return results

def moop_master():
    moop = moop_extract()
    results = refactor(moop,df,'MOOP')
    results = match_results(results, 'MOOP')
    # finished_duct = excel_gen(results, 'MOOP')
    # recolor_sheets('MOOP')
    results = static_result_description(results, 'MOOP')
    results = shrink_df(results, 'MOOP')

    return results

# bill = bill_master()
# duct = duct_master()
# moop = moop_master()

def master():
    benefits = bill_master()
    duct = duct_master()
    moop = moop_master()
    gen_excel(benefits, duct, moop)
    print('done')