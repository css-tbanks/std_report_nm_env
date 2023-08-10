from sql.alchemy_engine import *
from sql.main import *
from rule.rule_table_prep import *
from functions.benefit import *
from functions.main import *
from functions.static import *
from false_handling import *
from grexp import *


def bill_master():
    bill = bill_extract()
    results = benefit_refactor(bill,df)
    results = bene_match(results)
    # finished_bill = bene_excel(results)
    # recolor_sheets('Benefits')
    results = use_grexp(results, "bill")

    return results

def duct_master():
    duct = duct_extract()
    results = refactor(duct,df,'Deductible')
    results = match_results(results,'Deductible')
    # finished_duct = duct_excel(results)
    # recolor_sheets('Deductible')
    results = use_grexp(results, "duct")

    return results

def moop_master():
    moop = moop_extract()
    results = refactor(moop,df,'MOOP')
    results = match_results(results, 'MOOP')
    # finished_duct = excel_gen(results, 'MOOP')
    # recolor_sheets('MOOP')
    results = use_grexp(results, "moop")

    return results


def master():
    benefits = bill_master()
    duct = duct_master()
    moop = moop_master()
    # result = grexp_master(benefits, duct, moop)
    # print(result)
    gen_excel(benefits, duct, moop)





print('done')
# bill = bill_master()
# duct = duct_master()
# moop = moop_master()
# def grexp_master(benefits, duct, moop):
#     result = {}
#     result['benefits'] = use_grexp(benefits, "bill")
#     result['deductible'] = use_grexp(duct, "duct")
#     result['moop'] = use_grexp(moop, "moop")
#     return result