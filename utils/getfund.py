# -*- coding: UTF-8 -*-
import requests
from bs4 import BeautifulSoup
import re
import datetime
import os
import pandas as pd
import numpy as np

class GetData(object):
    def __init__(self):
        self.entry_per_page = 20
        self.duration = [str(datetime.date.today()-datetime.timedelta(days=365*8)),str(datetime.date.today())]
        self.single_fund_url = "http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code={fund_code}&page={page_num}&sdate={start_date}&edate={end_date}&per={entries_per_page}"
        self.all_funds_url="http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=1&lx=1&letter=&gsid=&text=&sort=zdf,desc&page=30,200&dt=1567696684255&atfc=&onlySale=0"
        self.parsed_data = None
        self.fund_folder = os.path.abspath(os.path.join(os.path.abspath(__file__),os.pardir, os.pardir,"data/fund_data"))
        return None
        
    def get_one_fund(self, fund_number, duration = [], entry_per_page = 20):
        if self.parsed_data is not None:
            self.parsed_data = None
        if len(duration) == 0:
            duration = self.duration
        def parse_request(text):
            plaintext = text[14:-2]
            pairs = plaintext.split(",")
            resp_dict = dict()
            for p in pairs:
                pair = p.split(":")
                resp_dict[pair[0]] = pair[1]
            return resp_dict
            
        def parse_content(content):
            content = content[1:-1]
            soup = BeautifulSoup(content,features="lxml")
            trs = soup.find_all("tr")
            templist = []
            for tr in trs:
                templist1 = [fund_number]
                for child in tr.contents:
                    if child.name == 'td':
                        if len(child.contents) == 0:
                            templist1.append("")
                        else:
                            templist1.append(child.contents[0])
                if len(templist1)>1:
                    templist.append(templist1)
            return templist
        fund_url = self.single_fund_url
        curpage, pages = 1,1
        total_list = []
        while True:
            resp = requests.get(fund_url.format(fund_code = str(fund_number), page_num = str(curpage), start_date = duration[0], end_date = duration[1], entries_per_page = str(entry_per_page)))
            resp_dict = parse_request(resp.text)
            parsed_data = parse_content(resp_dict["content"])
            total_list.extend(parsed_data)
            curpage = int(resp_dict["curpage"])
            pages = int(resp_dict["pages"])
            if curpage >= pages :
                break
            else:
                curpage+=1
        if self.parsed_data ==None:
            self.parsed_data = pd.DataFrame(total_list)
            self.parsed_data.columns = ["fund_code","date","price","accumulate","daily_rate","purchuse_state","ransom_state","dividends"]
        else:
            temp_data = pd.DataFrame(total_list)
            temp_data.columns = ["fund_code","date","price","accumulate","daily_rate","purchuse_state","ransom_state","dividends"]
            self.parsed_data = pd.concat([self.parsed_data.concat,temp_data], ignore_index = True)
        self.parsed_data["price"] = self.parsed_data[self.parsed_data["price"].str.contains(".")]['price'].astype(float)
        self.parsed_data['price'] = self.parsed_data["price"].fillna(self.parsed_data['price'].mean())
        self.parsed_data["accumulate"] = self.parsed_data[self.parsed_data["accumulate"].str.contains(".")]['accumulate'].astype(float)
        self.parsed_data["accumulate"] = self.parsed_data['accumulate'].fillna(self.parsed_data['accumulate'].mean())
        self.parsed_data["daily_rate"] = self.parsed_data[self.parsed_data["daily_rate"].str.contains("%")]['daily_rate'].str.strip("%").astype(float)/100
        self.parsed_data['daily_rate'] = self.parsed_data["daily_rate"].fillna(self.parsed_data['daily_rate'].mean())
        self.parsed_data["date"] = pd.to_datetime(self.parsed_data["date"])
        for column in self.parsed_data.columns:
            self.parsed_data[column] = self.parsed_data[column].astype(str)
        return self.parsed_data
        
    def get_funds(self, funds_list=[],duration=[]):
        if not duration:
            duration = self.duration
        if len(funds_list) == 0:
            return None
        fund_data = pd.DataFrame(columns=["fund_code","date","price","accumulate","daily_rate","purchuse_state","ransom_state"])
        for fund in funds_list:
            try:
                self.get_one_fund(fund, duration)
            except:
                print("error found in fund code: "+fund)
            else:
                fund_data = pd.concat([fund_data,self.parsed_data],ignore_index=True,sort=False)
        fund_data[["price","accumulate", "daily_rate"]] = fund_data[["price","accumulate", "daily_rate"]].astype(float)
        return fund_data
    
    def clear_parsed_data(self):
        self.parsed_data = None
        
    def create_increase_rate(self, parsed_data=None, base_date=None):
        if parsed_data is None:
            parsed_data = self.parsed_data
        if parsed_data is None or len(parsed_data.index) == 0:
            return None
        if base_date == None:
            base_date = parsed_data.iloc[parsed_data["date"].index[-1]]["date"]
        if base_date not in parsed_data["date"].value_counts().index:
            base_date = parsed_data.iloc[parsed_data["date"].index[-1]]["date"]
        base_prices = parsed_data[parsed_data["date"]==base_date]
        base_prices = base_prices.rename(columns={'price':'base_price'})
        parsed_data = pd.merge(parsed_data,base_prices[["fund_code","base_price"]],on=["fund_code","fund_code"],how='left')
        parsed_data["base_increase_rate"] = (parsed_data["price"]-parsed_data['base_price'])/parsed_data["base_price"]
        return parsed_data
    
    # 获取基金此刻的实时价格。在9：30-15：00期间的价格，如果超过15：00时是今日最终成交价，在9：30以前是昨日成交价。
    def get_realtime_price(self, fund_codes=[]):
        base_url = "http://fund.eastmoney.com/{}.html"
        realtime_prices = []
        for fund_code in fund_codes:
            url = base_url.format(fund_code)
            resp = requests.get(url)
            soup = BeautifulSoup(resp.text, 'html.parser')
            price = float(soup.find(id='gz_gsz').contents[0])
            realtime_prices.append(price)
        return realtime_prices
            
    # 获取距离今天最近一天的历史数据（不是实时数据，因为实时数据是变动数据，这里是还是从历史数据中抽取）
    def get_latest_price(self, parsed_data=None):    
        if parsed_data == None:
            parsed_data = self.parsed_data
        today = parsed_data["date"].iloc[0]
        funds = parsed_data["fund_code"].count().index
        price = dict()
        for fund in funds:
            price[fund] = parsed_data[parsed_data["fund_code"] == fund and parse_data["date"] == today]["price"].iloc[0]
        return pd.Series(price)
    
    def load_fund(self, fund_code = "", duration=[], file_path=""):
        if not file_path:
            file_path = os.path.join(self.fund_folder,fund_code+".pkl")
        if not os.path.exists(file_path):
            return None
        fund_data = pd.read_pickle(file_path)
        if not duration:
            return fund_data
        fund_data['date'] = pd.to_datetime(fund_data['date'])
        fund_data = fund_data.dropna()
        start_date = datetime.datetime.strptime(str(duration[0]),'%Y-%m-%d')
        end_date = datetime.datetime.strptime(str(duration[1]),'%Y-%m-%d')
        filtered_data = fund_data[(fund_data['date']<end_date) & (fund_data['date']>start_date)]
        filtered_data['date'] = filtered_data.date.apply(lambda x: x.strftime('%Y-%m-%d'))
        return filtered_data
    
    def load_funds(self, fund_codes=[], duration=[], fund_path=""):
        fund_data = None
        for fund_code in fund_codes:
            temp_fund_path = fund_path
            if temp_fund_path:
                if os.path.isdir(fund_path) and os.path.exists(fund_path):
                    temp_fund_path = os.path.abspath(os.path.join(fund_path,fund_code+".pkl"))
                else:
                    return False
            if fund_data is None:
                fund_data = self.load_fund(fund_code, duration, temp_fund_path)
            else:
                temp_fund_data = self.load_fund(fund_code, duration, temp_fund_path)
                fund_data = pd.concat([fund_data, temp_fund_data], axis=0, join='outer', ignore_index=True)
                fund_data.drop_duplicates(['fund_code','date'],keep='first',inplace=True)
                fund_data.reset_index(inplace=True)
        return fund_data
                
                
    def update_fund(self, fund_data=None, file_path=""):
        if fund_data is None:
            return False
        fund_code = fund_data.fund_code.iloc[0]
        if not file_path:
            file_path = os.path.join(self.fund_folder,fund_code+".pkl")
        if not os.path.exists(file_path):
            fund_data.to_pickle(file_path)
            print(fund_code+" is saved in "+file_path)
            return True
        else:
            old_data = pd.read_pickle(file_path)
            old_data = pd.concat([old_data,fund_data], axis=0,join="outer",ignore_index=True,sort=False)
            old_data.drop_duplicates(['fund_code','date'],keep='first', inplace=True)
            old_data.reset_index(inplace=True,drop=True)
            old_data.to_pickle(file_path)
            print(fund_code+" is updated and saved in "+file_path)
            return True
            
    def update_funds(self, fund_data, fund_path=""):
        fund_codes = fund_data.fund_code.value_counts().index
        for fund_code in fund_codes:
            if fund_path:
                if os.path.isdir(fund_path) and os.path.exists(fund_path):
                    fund_path =os.path.abspath(os.path.join(fund_path,fund_code+".pkl"))
                else:
                    return False
            fund = fund_data[fund_data.fund_code == fund_code]
            self.update_fund(fund, fund_path)
        return True
    
    def get_funds_list(self, file_path=""):
        if not file_path:
            file_path = os.path.join(self.fund_folder,"all_fund.csv")
        all_funds_table = pd.read_csv(file_path,dtype={'Num':np.int32, 'ID':np.str,'Name':np.str},index_col=0)
        return all_funds_table
        
# test_class = GetData()
# fund = test_class.get_funds(["110013","122"])
# fund = test_class.create_increase_rate(fund)

# realtime_price = test_class.get_realtime_price(['110013','005301'])
# print(realtime_price)