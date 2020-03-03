import requests
import json
import os, sys
import datetime
sys.setrecursionlimit(100000)
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.getfund import data_operator
# from utils.readconfig import config
# from models.models import db_operator
from utils.operatefund import *
# import datetime
# from bs4 import BeautifulSoup
import pandas as pd
from multiprocessing import Process, Queue, Pool
from models.models import db_operator
from concurrent.futures import ThreadPoolExecutor
# funds = ["110013", "000011","040001","020022"]
# count = 0
fds_list = data_operator.get_funds_list()
res = data_operator.get_realtime_price(fund_codes=fds_list.ID.values)
print(res)
# fund_in_db = db_operator.sql_query(sql = "SELECT distinct fund_code from Fund").fund_code.values
# ff = db_operator.sql_query("SELECT * from Fund where fund_code = '110013' Order by date DESC")
# print(ff)
# ff = db_operator.sql_query("SELECT * from Fund where fund_code = '110013' Order by date")
# ff = data_operator.load_funds(fund_codes=["110013","110031"],duration=[datetime.datetime(2018,1,2),datetime.datetime(2018,2,1)],orderby="date", asc = "DESC")
# ff = data_operator.load_fund(fund_code="110013", duration=[datetime.datetime(2018,1,1), datetime.datetime(2018,1,10)],orderby="price", asc="DESC")
# print(ff.dtypes, ff)
# print(fund_in_db)
# print("111",datetime.datetime.now())
# fund_data_3y = data_operator.load_funds(fund_codes=fund_in_db,duration=[datetime.datetime(2015,1,27), datetime.datetime(2018,1,25)])
# print("112",datetime.datetime.now())
# fund_data_2y = fund_data_3y[fund_data_3y.date>datetime.datetime(2015,1,27)+datetime.timedelta(days = 364)]
# print("113",datetime.datetime.now())
# print(len(fund_data_3y), len(fund_data_2y))
# print(fund_data_2y, fund_data_3y)
# db_operator.delete_user_by_id("test_000003_0000")
# fund_list = data_operator.get_funds_list()
# print(fund_list[fund_list.ID == "110013"].Name.iloc[0])
# fund_list = fund_list[fund_list.ID.isin(fund_in_db)]
# print(len(fund_list))
# all_fund = data_operator.get_funds_parallel(funds.ID.values)
# print("*** spider finished ***")
# db_operator.fund_df_to_table(fund_df=all_fund)

# data_operator.get_funds_parallel()


# b = data_operator.get_one_fund("110013")
# print(b)

class Trial(object):
    def __init__(self,):
        pass

    def read_multiple(self, fund_code,duration=[datetime.datetime(2019,12,15), datetime.datetime(2020,2,26)]):
        print("process to write for %s" % fund_code)
        try:
            fund = data_operator.get_one_fund(fund_code,duration)
        except:
            fund = None
            print("Error found in "+ fund_code)
            return fund
        else:
            print(fund_code + " is fetched")
            return fund

    def multiple_thread(self, funds):
        quedict = {}
        fund_sum = None
        with ThreadPoolExecutor(40) as executor:
            for each in funds:
                r1 = executor.submit(self.read_multiple, each)
                quedict[each] = r1
        for (x, y) in quedict.items():
            if fund_sum is None:
                fund_sum = y.result()
            else:
                if y.result() is not None:
                    print(x, " is being written to db")
                    # db_operator.fund_update(y.result())
                    fund_sum = pd.concat([fund_sum, y.result()], ignore_index=True, sort = False)
        return fund_sum

    def rewrite_get_funds(self, fund_list):
        p = Pool(10)
        quedict = {}
        for x in fund_list:
            a = p.apply_async(func = self.read_multiple, args = (x,))
            quedict[x] = a
        p.close()
        p.join()
        fund_sum = None
        for (x, y) in quedict.items():
            if fund_sum is None:
                fund_sum = y.get()
            else:
                if y.get() is not None:
                    fund_sum = pd.concat([fund_sum, y.get()], ignore_index=True, sort = False)
        return fund_sum

# user = User(username="zehua", password="0000", my_cash=1000)
# loaded_fund = data_operator.load_funds(fund_codes=["110031","110013"],duration=[datetime.datetime(2019,12,10), datetime.datetime(2019,12,17)])
# loaded_assets = data_operator.load_asset(userid="zehua_0000")
# loaded_records = data_operator.load_record(userid="zehua_0000")

# print("fund types:",loaded_fund.dtypes)
# print("assets types:",loaded_assets.dtypes)
# print("records types:",loaded_records.dtypes)
# print(loaded_fund[loaded_fund.fund_code =="110031"])

# operator = Operator(user)
# total_assets = operator.get_asset_value(date=datetime.datetime(2019,12,28),contains_cash=True,realtimeprice=False)
# print("total_assets: ",total_assets)





# loaded_data = data_operator.load_fund("110013")
# print(loaded_data.dtypes)

# if all_funds is None:
#     all_funds = y
# else:
#     if y is not None:
#         all_funds = pd.concat([all_funds,y],ignore_index=True,sort=False)

# def rewrite_get_funds(fund_list):
#     def read_multiple(fund_code):
#         print("process to write %s for %s" % os.getpid(), fund_code)
#         try:
#             fund = data_operator.get_one_fund(fund_code)
#         except:
#             fund = None
#             print("Error found in "+ fund_code)
#             return fund
#         else:
#             return fund
#     p = Pool(10)
#     quedict = {}
#     for x in fund_list:
#         a = p.apply_async(func = read_multiple, args = (x,))
#         quedict[x] = a.get()
#     p.close()
#     p.join()
#     return quedict
# def read_multiple(fund_code):
#     print("process to write: %s" % os.getpid())
#     try:
#         fund = data_operator.get_one_fund(fund_code)
#     except:
#         fund = None
#         print("Error found in "+ fund_code)
#         return fund
#     else:
#         return fund
# read_multiple("110013")

# def rewrite_get_funds(fund_list = []):
#     p = Pool(3)
#     quedict = {}
#     all_funds = None
#     print("before read_multiple starts")
#     def read_multiple(fund_code):
#         print("process to write: %s" % os.getpid())
#         try:
#             fund = data_operator.get_one_fund(fund_code)
#         except:
#             quedict[fund_code] = None
#             print("Error found in "+ fund_code)
#         else:
#             quedict[fund_code] = fund
    
#     for code in fund_list:
#         print(code)
#         p.apply_async(read_multiple, args=(code,))
#     p.close()
#     p.join()
#     print("Read multiple ends")
#     for (x, y) in quedict.items():
#         print("in loop")
#         if all_funds is None:
#             all_funds = y
#         else:
#             if y is not None:
#                 all_funds = pd.concat([all_funds,y],ignore_index=True,sort=False)
#     return all_funds
# a = rewrite_get_funds(fund_list = funds)
# print(a)

    

# def read_multiple(q, fund_code):
#     print("Process to write: %s" % os.getpid())
#     try:
#         fund = data_operator.get_one_fund(fund_code)
#     except:
#         print("Error fund in "+ fund_code)
#     else:
#         q.put(fund)
#     print(fund_code + "is fetched.")


# def merger(q, funds = None):
#     fund = q.get(True)
#     count = 0
#     if funds is None:
#         funds = fund
#     else:
#         funds = pd.concat([funds,fund],ignore_index=True,sort=False)
        
#     return funds    







# def f(name):
#     print("hello", name)

# p = Process(target = f, args = ("bob",))
# p.start()
# p.join()


# fundlist = data_operator.get_funds_list()
# data = data_operator.get_funds(fundlist.ID.values)
# print("getting all fund data done.")
# data_operator.update_fund(data)


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
