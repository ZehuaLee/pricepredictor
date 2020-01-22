import requests
import json
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.getfund import data_operator
from utils.readconfig import config
from models.models import db_operator
from utils.operatefund import *
import datetime
from bs4 import BeautifulSoup
import pandas as pd

fundlist = data_operator.get_funds_list()
data = data_operator.get_funds(fundlist.ID.values)
data_operator.update_fund(data)
# for fund_code in fundlist.ID:
#     print(fund_code)
#     fundpath = os.path.join(config["funds_folder"]+"funds",fund_code+".pkl")
#     data = data_operator.read_pickle(fundpath)
#     data_operator.update_fund(data)
#print (fundlist)
# data = data_operator.get_funds(funds_list = ["110013","000075"])
# data_operator.update_fund(data)
# parsed_data = getf.get_one_fund("110013",duration=[str(datetime.date.today()-datetime.timedelta(days=30*4)),str(datetime.date.today())])
# print(parsed_data, parsed_data.dtypes)
# getf.update_fund(parsed_data)
# data = getf.get_funds(['110013','001986'],[str(datetime.date.today()-datetime.timedelta(days=365*10)),str(datetime.date.today())])
# # data['date'] = pd.to_datetime(data['date'],format='%Y-%m-%d')
# # filtered_data = data[data['date']<pd.to_datetime('2019-08-01',format='%Y-%m-%d')]
# getf.update_funds(data)


#print(data)

# print( all_fund_code)

### how to predict a fund ###
# all_fund_code = getf.get_funds_list()
# for fund_code in all_fund_code['ID'].values:
#     one_fund_data = None
#     try:
#         one_fund_data = getf.get_one_fund(fund_code)
#     except Exception as e:
#         one_fund_data = None
#         print(fund_code+" is failed because of .")
#         print(e)
#     if one_fund_data is None:
#         continue
#     if '2012-09-07' in one_fund_data.date.values:
#         print(fund_code+" done.")
#         getf.update_fund(one_fund_data)
#     else:
#         print(fund_code+" is not the target.")
########################################




