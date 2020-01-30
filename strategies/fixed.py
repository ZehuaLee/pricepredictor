import os
import sys
sys.path.append(os.path.dirname(__file__))
from utils.getfund import data_operator
from utils.operatefund import Operator
from utils.readconfig import config
from models.models import db_operator
import datetime
import pandas as pd


class Strategy(object):
    def __init__(self, user):
        self.user = user
        
    
    def if_buy(self, fund_code, today_date = datetime.datetime(datetime.datetime.today().year, datetime.datetime.today().month,datetime.datetime.today().day), if_verify=True):
        start_date = (today_date - datetime.timedelta(days=365*3))
        duration = [start_date,today_date]
        past_3y_data = data_operator.load_fund(fund_code, duration=duration)
        past_3y_data['price'] = past_3y_data['price'].astype(float)
        past_2y_data = data_operator.load_fund(fund_code, duration=[(today_date-datetime.timedelta(days=365*2)), today_date])
        past_2y_data['price'] = past_2y_data['price'].astype(float)
        past_1y_data = data_operator.load_fund(fund_code, duration=[(today_date-datetime.timedelta(days=365)), today_date])
        past_1y_data['price'] = past_1y_data['price'].astype(float)
        y3_lowest_30_p = past_3y_data.sort_values(by=["price"])[:len(past_3y_data)*3//10]
        y2_lowest_30_p = past_2y_data.sort_values(by=["price"])[:len(past_3y_data)*3//10]
        y1_lowest_30_p = past_1y_data.sort_values(by=["price"])[:len(past_1y_data)*3//10]
        today_price = float('inf')
        if if_verify == True:
            today_price = data_operator.load_fund(fund_code,[(today_date-datetime.timedelta(days=1)), (today_date+datetime.timedelta(days=1))])
            today_price.price = today_price.price.astype(float)
            if len(today_price.index)==0:
                return 0
            today_price = today_price.price.iloc[0]
        else:
            today_price = data_operator.get_realtime_price(fund_codes=[fund_code])
            if len(today_price) == 0:
                return 0
            today_price = today_price[0]
        buy_prob = 0
        if today_price < y3_lowest_30_p.price.iloc[-1]:
            buy_prob = len(y3_lowest_30_p[y3_lowest_30_p.price>today_price])/len(y3_lowest_30_p)
        elif today_price < y2_lowest_30_p.price.iloc[-1]:
            buy_prob = (len(y2_lowest_30_p[y2_lowest_30_p.price>today_price])/len(y2_lowest_30_p))*0.66
        elif today_price < y1_lowest_30_p.price.iloc[-1]:
            buy_prob = (len(y1_lowest_30_p[y1_lowest_30_p.price>today_price])/len(y1_lowest_30_p))*0.33
        else:
            buy_prob = 0
        records = self.load_records()
        if "cost" in records.keys():
            if fund_code not in records['cost']:
                avg_cost = 0
            elif records['units'][fund_code] == 0:
                avg_cost = 0
            else:    
                avg_cost = records["cost"][fund_code]/records["units"][fund_code]
        else:
            avg_cost = 0
        if avg_cost == 0:
            return buy_prob 
        # print(type(today_price))
        buy_prob = buy_prob+(avg_cost-today_price)/avg_cost
        return buy_prob
        
        
    def if_sell(self, fund_code, today_date, if_verify=True):
        today_date = datetime.datetime.strptime(today_date,'%Y-%m-%d').date()
        start_date = (today_date - datetime.timedelta(days=365*3))
        duration = [str(start_date),str(today_date)]
        past_3y_data = self.getfd.load_fund(fund_code, duration=duration)
        past_3y_data.price = past_3y_data.price.astype(float)
        # past_2y_data = self.getfd.load_fund(fund_code, duration=[(today_date-datetime.timedelta(days=365*2)).date(), today_date])
        # past_1y_data = self.getfd.load_fund(fund_code, duration=[(today_date-datetime.timedelta(days=365)).date(), today_date])
        y3_top_30_p = past_3y_data.sort_values(by=["price"],ascending=False)[:len(past_3y_data)*2//10]
        records = self.load_records()
        if 'cost' in records.keys() and 'units' in records.keys():
            if fund_code in records["cost"].index and fund_code in records['units'].index:
                if records["units"][fund_code] == 0:
                    avg_cost = 0
                else:
                    avg_cost = records["cost"][fund_code]/records["units"][fund_code]
            else:
                avg_cost = 0
        else:
            avg_cost = 0
        today_price = 0
        if if_verify == True:
            today_price = self.getfd.load_fund(fund_code,[(today_date-datetime.timedelta(days=1)), (today_date+datetime.timedelta(days=1))])
            today_price.price = today_price.price.astype(float)
            if len(today_price.index)==0:
                return 0
            today_price = today_price.price.iloc[0]
        else:
            today_price = self.getfd.get_realtime_price(fund_codes=[fund_code])
            if len(today_price) == 0:
                return 0
            today_price = today_price[0]
        sell_prob = 0
        if avg_cost <=0:
            return 0
        # print("---", today_price, avg_cost)
        # print(type(today_price),type(avg_cost))
        if (today_price-avg_cost)/avg_cost > 0.1:
            sell_prob = (today_price-avg_cost)/avg_cost
            return 0.7
        return 0
            
            
        
    # def exec_strategy(self, fund_codes=[], date=''):
    #     if not fund_codes:
    #         self.fund_codes = fund_codes
    #     if not date:
    #         self.date = date
    #     if not self.fund_codes:
    #         return False
    #     if not self.date:
    #         date = str(datetime.date.today())
    #     records = []
    #     for fund in fund_codes:
    #         record = self.single_stragety(fund, date)
    #         records.append(record)
    
    def load_records(self, record_path = ""):
        if not record_path:
            record_path = self.record_folder
        result = dict()
        if os.path.exists(os.path.join(record_path, "units.pkl")):
            result["units"] = pd.read_pickle(os.path.join(record_path, "units.pkl"))
        if os.path.exists(os.path.join(record_path, "cost.pkl")):
            result["cost"] = pd.read_pickle(os.path.join(record_path, "cost.pkl"))
        if os.path.exists(os.path.join(record_path, "benefit.pkl")):
            result["benefit"] = pd.read_pickle(os.path.join(record_path, "benefit.pkl"))
        if os.path.exists(os.path.join(record_path, "records.pkl")):
            result["records"] = pd.read_pickle(os.path.join(record_path, "records.pkl"))
        return result
        
            
    # def single_stragety(self, fund_code, date, is_verify = True):
    #     fixed_value = 100
    #     get_data = GetData()
    #     end_date = datetime.datetime.strptime(date,'%Y-%m-%d').date()
    #     start_date = end_date-datetime.timedelta(weeks=150)
    #     historic_data = get_data.get_one_fund(fund_code, duration = [str(start_date), str(end_date)])
    #     real_data = None
    #     if is_verify == True:
    #         if date in historic_data.date.value_counts().index:
                
    #         real_data = historic_data.iloc[0]
            
        
    #     return [fund_code, date, buy_sell, share]     

# test_strategy = strategy()
# a = test_strategy.if_buy("110013",'2018-12-10')
# print(a)