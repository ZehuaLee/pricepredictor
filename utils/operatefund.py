import pandas as pd
import numpy as np
import datetime
import getfund
import os
import sys
sys.path.append(os.path.abspath(os.path.join(__file__, "../../strategies")))

class Operation(object):
    def __init__(self, total_data = None):
        self.all_funds = []
        self.share = 100
        self.benefit = pd.Series()
        self.benefit['total_benefit'] = 0
        self.units = pd.Series()
        self.cost = pd.Series()
        self.values = pd.Series()  # the value of unit* current price
        self.records = pd.DataFrame(columns = ["fund_code","date","price","accumulate","units","buy_sell", "sold"])
        self.target_fund = None
        self.total_data = total_data
        self.latest_price = pd.Series()
        self.record_folder = os.path.join(os.path.split(os.path.split(__file__)[0])[0], "data/op_records")
        self.getdata = getfund.GetData()

    def clear_operations(self):
        self.total_data = None
        self.records = pd.DataFrame(columns = ["fund_code","date","price","accumulate","units","buy_sell", "sold"])
        self.target_fund = None
        self.latest_price = pd.Series()
        self.benefit = pd.Series()
        self.units = pd.Series()
        self.cost = pd.Series()
        self.values = pd.Series()
    
    def verify_strategy_on_single_fund(self, mystrategy = None, fund_code="", test_data = None):
        if test_data is None:
            print("can not verify strategy with no test data found.")
            return False
        if mystrategy is None:
            print("can not verify strategy with no strategy found.")
            return False
        if not fund_code:
            fund_code = test_data.fund_code.iloc[0]
        self.clear_operations()
        for today in test_data.index:
            # print("(((", test_data.loc[today].date, today,str(test_data.loc[today].date))
            # print()
            buy_prob = mystrategy.if_buy(test_data.loc[today].fund_code, str(test_data.loc[today].date), if_verify=True)
            sell_prob = mystrategy.if_sell(test_data.loc[today].fund_code, str(test_data.loc[today].date), if_verify=True)
            opt = "no"
            # if (buy_prob >0 or sell_prob >0):
            #     print("****************: ", buy_prob, sell_prob)
            if buy_prob > 0.6 or sell_prob > 0.6:
                opt = "buy" if buy_prob>= sell_prob else "sell"
            if opt == "sell":
                print("bought")
                # def sell_at_date(self, fund_code, date, amount=0, if_value=True):
                self.sell_at_date(test_data.loc[today].fund_code, str(test_data.loc[today].date),amount=self.share, if_value=True )
            if opt == "buy":
                print("sold")
                # def buy_at_date(self, fund_code, date, amount=0, if_value=True)
                self.buy_at_date(test_data.loc[today].fund_code, str(test_data.loc[today].date),amount=self.share, if_value=True)
        
        test_data.date = pd.to_datetime(test_data.date)
        test_data.sort_values(by = 'date', inplace=True)
        total_asset = self.get_total_assets(test_data,self.units,date = str(test_data.iloc[-1].date.date()))
        if fund_code in self.cost.index:
            gain = total_asset-self.cost[fund_code]
        else:
            print("no fund_code of ", fund_code)
            # print("Cost Values ",self.cost.values)
            # print("Records ",self.records.values)
            gain = total_asset
        yearly_gain = gain/((test_data.iloc[-1].date-test_data.iloc[0].date).days/365)
        cost = 0
        if fund_code not in self.cost:
            yearly_gain_rate = float('inf')
        else:
            cost = self.cost[fund_code]
            yearly_gain_rate = yearly_gain/self.cost[fund_code]
        return [gain, yearly_gain, yearly_gain_rate, cost]
        
    def verify_strategy(self, mystrategy = None, fund_codes=[], test_data=None):
        result = dict()
        for fund in fund_codes:
            fund_data = test_data[test_data.fund_code==fund]
            result[fund] = self.verify_strategy_on_single_fund(mystrategy, fund, test_data=fund_data)
        return result

    def load_operations(self, record_path=""):
        if not record_path:
            record_path = self.record_folder
        self.units = pd.read_pickle(os.path.join(record_folder, "units.pkl"))
        self.cost = pd.read_pickle(os.path.join(record_folder, "cost.pkl"))
        self.benefit = pd.read_pickle(os.path.join(record_folder, "benefit.pkl"))
        self.records = pd.read_pickle(os.path.join(record_folder, "records.pkl"))
        self.all_funds = self.units.index
    
    def save_operations(self, record_path=""):
        if not record_path:
            record_path = self.record_folder
        self.units.to_pickle(os.path.join(record_path, "units.pkl"))
        self.cost.to_pickle(os.path.join(record_path, "cost.pkl"))
        self.benefit.to_pickle(os.path.join(record_path, "benefit.pkl"))
        self.records.to_pickle(os.path.join(record_path, "records.pkl"))
        
    def off_work(self, folder = ""):
        self.save_operations(folder)
        print(str(datetime.date.today()) + "'s job has been done, now off work.")
        
    def start_work(self, folder = ""):
        self.load_operations(folder)
        print(str(datetime.date.today()) + "'s job started.")
        
    def get_latest_price(self, fund_data = None):
        if fund_data == None:
            fund_data = self.total_data
        latest_day = self.find_nearst_date(date_string = str(datetime.date.today()), fund_data = fund_data)
        price = fund_data[fund_data["date"] == latest_day][["price","fund_code"]].set_index("fund_code")['price']
        return price
    
    # 获取当前持有基金份额的价值
    def get_values(self, fund_data = None, units = None, date=str(datetime.date.today())):
        values = units.copy()
        if fund_data is None:
            fund_data == self.total_data
        if units is None:
            units = self.units
        day = self.find_nearest_date(date_string = date, fund_data = fund_data)
        price = fund_data[fund_data["date"] == day][["price","fund_code"]].set_index("fund_code")['price']
        
        for fund in units.index:
            # print("{{{{{",price[fund])
            values[fund] = values[fund]*price[fund]
        if "total_value" not in values.index:
            values['total_value'] = 0
        # print("]]]",values['002001'])
        values["total_value"] = values.sum()-values["total_value"]
        self.values = values.copy()
        return values
    
    # 获取总资产 = 卖掉的基金收益（benefit） + 当前持有基金的份额的价值（total_values）
    def get_total_assets(self, fund_data = None, units = None, date=str(datetime.date.today())):
        fund_values = self.get_values(fund_data, units, date)
        if 'total_benefit' in self.benefit.index:
            benefits = self.benefit["total_benefit"]
        else:
            benefits = 0
        return fund_values['total_value']+benefits
    
    def print_assets(self):
        print("units: ", self.units)
        print("cost: ", self.cost)
        self.latest_price = self.get_latest_price()
        self.values = self.get_values()
        print("latest price: ", self.latest_price)
        print("values: ", self.values)
        print("benefit: ", self.benefits)
        print("Timestamp: ", str(datetime.datetime.now()))
    
    # 在某日买入若干份额基金     
    def buy_at_date(self, fund_code, date, amount=0, if_value=True):
        share_value = amount
        share_units = amount
        fund_price = float('inf')
        fund_accumulate = float('inf')
        share_value = 0
        share_units = 0
        # print("self.total_data ",self.total_data)
        if self.total_data is None:
            self.total_data = self.getdata.load_fund(fund_code)
            self.total_data['price'] = self.total_data['price'].astype(float)
        # print("self total data ", self.total_data, self.total_data.dtypes)
        if self.total_data is not None:
            fund_price = self.total_data[(self.total_data["date"]==date)&(self.total_data['fund_code']==fund_code)]
            fund_price.price = fund_price.price.astype(float)
            fund_price = fund_price["price"].iloc[0]
            fund_accumulate = self.total_data[(self.total_data["date"]==date) & (self.total_data['fund_code']==fund_code)]["accumulate"].iloc[0]
        else:
            raise Exception("please give a total data to operation")
        if if_value:
            # print("amount: ",type(amount))
            # print("fund_price: ",type(fund_price))
            share_value = amount
            share_units = share_value/fund_price
        else:
            share_value = amount*fund_price
            share_units = amount
        # except Exception:
        #     fund_price = float('inf')
        #     share_units = 0
        #     share_value = 0
        #     print(fund_code+' is an exception on '+ date)
        self.records.loc[len(self.records.index)] = [fund_code, date, fund_price, fund_accumulate, share_units, 'buy', False]
        if fund_code not in self.units.index:
            self.units[fund_code] = share_units
        elif fund_code in self.units.index:
            self.units[fund_code] +=share_units
        else:
            self.units[fund_code] = share_units
        if fund_code is None:
            self.cost[fund_code] = share_value
        elif fund_code in self.cost.index:
            self.cost[fund_code]+=share_value
        else:
            self.cost[fund_code] = share_value
        if 'total_cost' not in self.cost.index:
            self.cost["total_cost"] = 0
        self.cost["total_cost"] = self.cost.sum()-self.cost['total_cost']
        if fund_code not in self.all_funds:
            self.all_funds.append(fund_code)
        self.save_operations()
        return True
    
    
    # 在某日卖出若干份额基金 
    def sell_at_date(self, fund_code, date, amount=0, if_value=True):
        share_value = amount
        share_units = amount
        if self.total_data is None:
            self.total_data = self.getdata.load_fund(fund_code)
        fund_price = self.total_data[(self.total_data["date"]==date)&(self.total_data['fund_code']==fund_code)]
        fund_price.price = fund_price.price.astype(float)
        fund_price = fund_price["price"].iloc[0]
        fund_accumulate = self.total_data[(self.total_data["date"]==date) & (self.total_data['fund_code']==fund_code)]["accumulate"].iloc[0]
        if (fund_code in self.cost) and (fund_code in self.units):
            average_cost = self.cost[fund_code]/self.units[fund_code]
        else:
            average_cost = 0
        if if_value:
            share_value = amount
            share_units = share_value/fund_price
        else:
            share_value = amount*fund_price
            share_units = amount
        if fund_code not in self.units or share_units > self.units[fund_code]:
            return False
        self.cost[fund_code] = self.cost[fund_code] - share_units*average_cost
        if fund_code not in self.benefit.index:
            self.benefit[fund_code] = share_value - share_units*average_cost
        else:
            self.benefit[fund_code] += share_value - share_units*average_cost
        if "total_benefit" not in self.benefit.index:
            self.benefit["total_benefit"] = self.benefit.sum()
        self.benefit["total_benefit"] = self.benefit.sum() - self.benefit['total_benefit']
        if fund_code not in self.units.index:
            self.units[fund_code] = 0
        else:
            self.units[fund_code] = self.units[fund_code] - share_units
        if 'total_cost' not in self.cost.index:
            self.cost['total_cost'] = self.cost.sum()
        else:    
            self.cost["total_cost"] = self.cost.sum()-self.cost['total_cost']
        # self.records = pd.DataFrame(columns = ["fund_code","date","price","accumulate","units","buy_sell", "sold"])
        self.records.loc[len(self.records.index)] = [fund_code, date, fund_price, fund_accumulate, share_units, 'sell', True]
        if self.units[fund_code] == 0:
            del self.units[fund_code]
            del self.units[fund_code]
            self.all_funds.remove(fund_code)
        self.save_operations()
        return True
    
    # 找到最距离目标日期最近的一天，latest是目标日期前最近一天还是目标日期后最近一天
    def find_nearest_date(self, date_string=None, fund_data = None, latest=True):
        if fund_data is None:
            fund_data = self.total_data
        if date_string is None:
            date_string = str(datetime.date.today())
        dates = fund_data["date"].value_counts().index
        if date_string in dates:
            return date_string
        else:
            day = datetime.datetime.strptime(date_string,'%Y-%m-%d')
            days = [datetime.datetime.strptime(x,'%Y-%m-%d') for x in dates].sort()
            if latest:
                days = days.sort()
                nearst_date = days[-1]
                for x in days:
                    if x > day:
                        nearst_date = x
                        break
                return str(nearst_date.date())
            else:
                days = days.sort(reverse=True)
                nearst_date = days[-1]
                for x in days:
                    if x < day:
                        nearst_date = x
                        break
                return str(nearst_date.date())
    
    

    
# getdata = getfund.GetData()
# fund = getdata.get_funds(["110013"])
# fund = getdata.create_increase_rate(fund)
# op = operation(fund)
# op.buy_at_date(fund_code='110013',date='2019-08-26',amount=100)
# op.sell_at_date(fund_code="110013", date="2019-08-27",amount=100)
# print(op.records)
# print(os.path.join(os.path.split(os.path.split(__file__)[0])[0], "data/op_records"))