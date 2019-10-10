import requests
import json
import getfund
import datetime
from bs4 import BeautifulSoup
import pandas as pd

getf = getfund.GetData()

parsed_data = getf.get_one_fund("110013")
print(parsed_data)
getf.update_fund(parsed_data)
# data = getf.get_funds(['110013','001986'],[str(datetime.date.today()-datetime.timedelta(days=365*10)),str(datetime.date.today())])
# # data['date'] = pd.to_datetime(data['date'],format='%Y-%m-%d')
# # filtered_data = data[data['date']<pd.to_datetime('2019-08-01',format='%Y-%m-%d')]
# getf.update_funds(data)
# data = getf.load_funds(['110013','001986'])

# print(data.fund_code.value_counts())

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




