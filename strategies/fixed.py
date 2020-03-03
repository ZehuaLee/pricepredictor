import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.getfund import data_operator
from utils.operatefund import Operator, User
from utils.readconfig import config
from models.models import db_operator
import datetime
import pandas as pd


class Strategy(object):
    def __init__(self, user):
        self.user = user
        
    def if_buy(self, fund_code, today_date = datetime.datetime(datetime.datetime.today().year, datetime.datetime.today().month,datetime.datetime.today().day), if_verify=True, all_fund_data = None):
        # 获取3年内基金数据价格最低Top30%的数据
        start_date = (today_date - datetime.timedelta(days=365*3))
        yesterday_date = today_date-datetime.timedelta(days=1)
        duration_3y = [start_date,yesterday_date]
        duration_2y = [start_date+datetime.timedelta(days=365), yesterday_date]
        duration_1y = [start_date+datetime.timedelta(days=365*2),yesterday_date]
        # past_3y_data = db_operator.sql_query(sql="SELECT * from Fund where fund_code = '110013' Order by date")
        past_3y_data = data_operator.load_fund(fund_code, duration=duration_3y,orderby="price",asc="ASC")
        past_2y_data = data_operator.load_fund(fund_code=fund_code,duration=duration_2y,orderby="price",asc="ASC")
        past_1y_data = data_operator.load_fund(fund_code=fund_code, duration=duration_1y, orderby="price",asc="ASC")
        # 获取今日价格，若是测试则使用历史数据，否则用当日实时数据
        today_price = float('inf')
        if if_verify == True:
            today_price = data_operator.load_fund(fund_code=fund_code,duration=[today_date, today_date])
            if len(today_price.index)==0:
                return 0
            today_price = today_price.price.iloc[0]
        else:
            today_price = data_operator.get_realtime_price(fund_codes=[fund_code])
            if len(today_price) == 0:
                return 0
            today_price = today_price[0]
        # past_3y_data['price'] = past_3y_data['price'].astype(float)

        # past_2y_data['price'] = past_2y_data['price'].astype(float)
        # past_1y_data['price'] = past_1y_data['price'].astype(float)
        y3_lowest_30_p = past_3y_data[:len(past_3y_data)*3//10]
        y2_lowest_30_p = past_2y_data[:len(past_2y_data)*3//10]
        y1_lowest_30_p = past_1y_data[:len(past_1y_data)*3//10]
        # 计算当前价格在3年内最低30%的价格中的排名占比
        buy_prob = 0
        if not y2_lowest_30_p.empty and today_price < y3_lowest_30_p.price.iloc[-1]:
            buy_prob = (len(past_3y_data)/(365*3))*len(y3_lowest_30_p[y3_lowest_30_p.price>today_price])/len(y3_lowest_30_p)
        elif not y2_lowest_30_p.empty and today_price < y2_lowest_30_p.price.iloc[-1]:
            buy_prob = ((len(past_2y_data)/(365*2))*len(y2_lowest_30_p[y2_lowest_30_p.price>today_price])/len(y2_lowest_30_p))*0.66
        elif not y1_lowest_30_p.empty and today_price < y1_lowest_30_p.price.iloc[-1]:
            buy_prob = ((len(past_1y_data)/365)*len(y1_lowest_30_p[y1_lowest_30_p.price>today_price])/len(y1_lowest_30_p))*0.33
        else:
            buy_prob = 0
        # 读取用户资产和购买记录
        assets = self.user.asset
        records = self.user.record
        # if not records.empty:
        #     records.date = pd.to_datetime(records.date)
        # 判断今日价格是否比15天内的购买价格都低，如果15天以内买入过，那么就仅在当前价格比15内买入价格更低时买入。
        rec_in_days = records[(records.date>(today_date-datetime.timedelta(days=5))) & (records.fund_code == fund_code) & (records.buy_sell == "buy")]
        prob_plus = 0
        if len(rec_in_days[rec_in_days.price<=today_price])>0:
            prob_plus = 0
        else:
            prob_plus = 1
        target_fund = assets[assets.my_fund == fund_code]
        avg_cost = 0
        if target_fund.empty :
            avg_cost = 0
        else:
            avg_cost = target_fund.my_cost.iloc[0]/target_fund.my_units.iloc[0]
        if avg_cost == 0:
            return buy_prob
        buy_prob = buy_prob+(avg_cost-today_price)/avg_cost
        if prob_plus > 0:
            return buy_prob
        else:
            return 0

    def if_sell(self, fund_code, today_date, if_verify=True, all_data_fund = None):
        # 获取3年内基金数据价格最高Top30%的数据
        start_date = (today_date - datetime.timedelta(days=365*3))
        duration = [start_date,today_date]
        past_3y_data = all_data_fund
        if past_3y_data is None:
            past_3y_data = data_operator.load_fund(fund_code, duration=[start_date, today_date-datetime.timedelta(days=1)],orderby="price",asc="DESC")
        # past_3y_data.price = past_3y_data.price.astype(float)
        today_price = 0
        if if_verify == True:
            today_price = data_operator.load_fund(fund_code=fund_code,duration=[today_date, today_date])
            if len(today_price.index)==0:
                return 0
            today_price = today_price.price.iloc[0]
        else:
            today_price = data_operator.get_realtime_price(fund_codes=[fund_code])
            if len(today_price) == 0:
                return 0
            today_price = today_price[0]
        y3_top_30_p = past_3y_data[:len(past_3y_data)*3//10]
        # 获取用户的资产数据和购买记录
        assets = self.user.asset
        records = self.user.record
        avg_cost = 0
        if assets.empty:
            return 0
        target_fund = assets[assets.my_fund == fund_code]
        if target_fund.empty:
            return 0
        else:
            avg_cost = target_fund.my_cost.iloc[0]/target_fund.my_units.iloc[0]
        # 如果5天以内卖出过，那么就仅在当前价格比15内卖出价格更高时卖出。
        rec_in_days = records[(records.date>(today_date-datetime.timedelta(days=5))) & (records.fund_code == fund_code) & (records.buy_sell == "sell")]
        prob_plus = 0 
        # 用来判断今日价格是否高于15日以内所有卖出记录的价格
        if len(rec_in_days[rec_in_days.price>=today_price])>0:
            prob_plus = 0
        else:
            prob_plus = 1
        # 计算当前价格在3年内最高30%的价格中的排名占比        
        sell_prob = 0
        if avg_cost <=0:
            sell_prob = 0
            return sell_prob
        sell_prob = len(y3_top_30_p[y3_top_30_p.price < today_price])/len(y3_top_30_p)
        # 判断是否高于成本50%以上，（保证收益50%以上才卖出）
        if (today_price-avg_cost)/avg_cost >= 0.2:
            sell_prob = (today_price-avg_cost)/avg_cost + sell_prob
            if prob_plus >0:
                return sell_prob
            else:
                return 0
        return 0

    def if_buy_accelerated(self, fund_list, today_date, if_verify=True, all_data_fund = None):
        dur = [today_date-datetime.timedelta(days=365*3), today_date]
        fds_all_3y = data_operator.load_funds(fund_codes=fund_list, duration=dur, orderby="price", asc="asc")
        if if_verify:
            today_price = fds_all_3y[fds_all_3y.date == today_date].set_index("fund_code")
        else:
            today_price = data_operator.get_realtime_price(fund_list).set_index("fund_code")
        if len(today_price) ==0:
            return []
        fds_all_2y = fds_all_3y[fds_all_3y.date > today_date-datetime.timedelta(days=365*2)]
        fds_all_1y = fds_all_2y[fds_all_2y.date > today_date-datetime.timedelta(days=365)]
        # res_3y = fds_all_3y.groupby(by="fund_code").apply(lambda x: len(x[:len(x)*3//10][x[x.date==today_date].price.iloc[0]<x.price])/len(x[:len(x)*3//10]) if len(x[:len(x)*3//10])>0 and len(x[x.date==today_date])>0 else 0)
        res_3y = fds_all_3y.groupby(by="fund_code").apply(lambda x: len(x[:len(x)*3//10][x[x.date==today_date].price.iloc[0]<x.price])/240 if len(x[:len(x)*3//10])>0 and len(x[x.date==today_date])>0 else 0)
        res_2y = fds_all_2y.groupby(by="fund_code").apply(lambda x: len(x[:len(x)*3//10][x[x.date==today_date].price.iloc[0]<x.price])/160 if len(x[:len(x)*3//10])>0 and len(x[x.date==today_date])>0 else 0)*0.666
        res_1y = fds_all_1y.groupby(by="fund_code").apply(lambda x: len(x[:len(x)*3//10][x[x.date==today_date].price.iloc[0]<x.price])/80 if len(x[:len(x)*3//10])>0 and len(x[x.date==today_date])>0 else 0)*0.333
        result_list = res_3y[res_3y>0.8]
        result_list = result_list.append(res_2y[(res_2y>0.6) & (~(res_2y.index.isin(result_list.index)))])
        result_list = result_list.append(res_1y[(res_1y>0.3) & (~(res_1y.index.isin(result_list.index)))])
        assets = self.user.asset
        records = self.user.record
        rec_in_days = records[(records.date>(today_date-datetime.timedelta(days=20))) & (records.fund_code.isin(fund_list)) & (records.buy_sell == "buy")].set_index("fund_code")
        # fund_not_buy = (rec_in_days[["price"]]-today_price[["price"]]).fillna(0).apply(lambda x: x[x<0])
        # fund_not_buy = fund_not_buy[~fund_not_buy.index.duplicated()].index
        fund_not_buy = rec_in_days.index.values
        avg_cost = assets[["my_fund","my_units","my_cost"]].rename(columns={"my_fund":"fund_code"}).set_index("fund_code")
        avg_cost["avg_cost"] = avg_cost["my_cost"]/avg_cost["my_units"]
        avg_cost_prob = ((avg_cost["avg_cost"]-today_price["price"])/avg_cost["avg_cost"]).fillna(0)
        avg_cost_prob = avg_cost_prob[avg_cost_prob!=0].to_frame(name="prob")
        result_list = result_list.to_frame(name="prob")
        result_list = result_list[~result_list.index.isin(fund_not_buy)]
        # 以1元作标准，对结果作比例加成
        # funds_on_sale = today_price[today_price.price]-1
        for x in avg_cost_prob.index:
            if x in result_list.index:
                result_list.prob.loc[x] += avg_cost_prob.prob.loc[x]
        return result_list[result_list.prob>0.85].fillna(0).sort_values("prob", ascending=False)[:10]

    def if_sell_accelerated(self, fund_list, today_date, if_verify=True, all_data_fund = None):
        dur = [today_date-datetime.timedelta(days=365*3), today_date]
        assets = self.user.asset
        records = self.user.record
        if assets.empty:
            return []
        fds_all_3y = data_operator.load_funds(fund_codes=fund_list, duration=dur, orderby="price", asc="desc")
        if if_verify:
            today_price = fds_all_3y[fds_all_3y.date == today_date].set_index("fund_code")
        else:
            today_price = data_operator.get_realtime_price(fund_list)
        if len(today_price) ==0:
            return []
        res_3y = fds_all_3y.groupby(by="fund_code").apply(lambda x: len(x[:len(x)*3//10][x[x.date==today_date].price.iloc[0]>x.price])/len(x[:len(x)*3//10]) if len(x[:len(x)*3//10])>0 and len(x[x.date==today_date])>0 else 0)
        result_list = res_3y[res_3y>0.7]
        rec_in_days = records[(records.date>(today_date-datetime.timedelta(days=5))) & (records.fund_code.isin(fund_list)) & (records.buy_sell == "sell")].set_index("fund_code")
        fund_not_buy = (rec_in_days[["price"]]-today_price[["price"]]).fillna(0).apply(lambda x: x[x<0])
        fund_not_buy = fund_not_buy[~fund_not_buy.index.duplicated()].index
        avg_cost = assets[["my_fund","my_units","my_cost"]].rename(columns={"my_fund":"fund_code"}).set_index("fund_code")
        avg_cost["avg_cost"] = avg_cost["my_cost"]/avg_cost["my_units"]
        avg_cost_prob = ((today_price["price"]-avg_cost["avg_cost"])/avg_cost["avg_cost"]).fillna(0)
        avg_cost_prob = avg_cost_prob[avg_cost_prob>0.05].to_frame(name="prob")
        result_list = result_list.to_frame(name="prob")
        result_list = result_list[(~result_list.index.isin(fund_not_buy)) & (result_list.index.isin(avg_cost_prob.index))]
        for x in avg_cost_prob.index:
            if x in result_list.index:
                result_list.prob.loc[x] += avg_cost_prob.prob.loc[x]
        return result_list[result_list.prob>0.7].fillna(0).sort_values("prob", ascending=False)[:10]
        
    def if_sell_accelerated_2(self, fund_list, today_date, if_verify=True, all_data_fund = None):
        dur = [today_date, today_date]
        assets = self.user.asset
        if if_verify:
            today_price = data_operator.load_funds(fund_codes=fund_list, duration=dur, orderby="price", asc="desc")
        else:
            today_price = data_operator.get_realtime_price(fund_list)
        if today_price.empty:
            return []
        if assets.empty:
            return []
        avg_cost = assets[["my_fund","my_units","my_cost"]].rename(columns={"my_fund":"fund_code"}).set_index("fund_code")
        avg_cost["avg_cost"] = avg_cost["my_cost"]/avg_cost["my_units"]
        avg_cost_prob = ((today_price["price"]-avg_cost["avg_cost"])/avg_cost["avg_cost"]).fillna(0)
        avg_cost_prob = avg_cost_prob[avg_cost_prob>0.115].to_frame(name="prob").sort_values("prob")
        return avg_cost_prob[:10]

    def load_records(self, user = None):
        if user is None:
            user = self.user
        myRecords = user.record
        return myRecords
    
    def load_assets(self, user = None):
        if user is None:
            user = self.user
        myAssets = data_operator.load_asset(user.userid)
        return myAssets



# def if_sell(self, fund_code, today_date, if_verify=True, all_data_fund = None):
#         # 获取3年内基金数据价格最高Top30%的数据
#         start_date = (today_date - datetime.timedelta(days=365*3))
#         duration = [start_date,today_date]
#         past_3y_data = all_data_fund
#         if past_3y_data is None:
#             past_3y_data = data_operator.load_fund(fund_code, duration=duration)
#         # past_3y_data.price = past_3y_data.price.astype(float)
#         today_price = 0
#         if if_verify == True:
#             today_price = past_3y_data[past_3y_data.date == today_date]
#             if len(today_price.index)==0:
#                 return 0
#             today_price = today_price.price.iloc[0]
#         else:
#             today_price = data_operator.get_realtime_price(fund_codes=[fund_code])
#             if len(today_price) == 0:
#                 return 0
#             today_price = today_price[0]
#         past_3y_data[past_3y_data.date>today_date].sort_values(by=["price"],ascending=False).reset_index(drop=True,inplace = True)
#         y3_top_30_p = past_3y_data[:len(past_3y_data)*3//10]

#         # 获取用户的资产数据和购买记录
#         assets = self.user.asset
#         records = self.user.record
#         avg_cost = 0
#         if assets.empty:
#             # print("oper_asset:", assets)
#             # print("oper_record:",records)
#             return 0
#         target_fund = assets[assets.my_fund == fund_code].reset_index(drop=True, inplace = True)
#         if target_fund.empty:
#             return 0
#         else:
#             avg_cost = target_fund.my_cost.iloc[0]/target_fund.my_units.iloc[0]
#         # 获取当日基金价格，若是测试则使用历史数据，否则用当日实时数据
        
#         # 如果5天以内卖出过，那么就仅在当前价格比15内卖出价格更高时卖出。
#         rec_in_days = records[(records.date>(today_date-datetime.timedelta(days=5))) & (records.fund_code == fund_code) & (records.buy_sell == "sell")]
#         prob_plus = 0 # 用来判断今日价格是否高于15日以内所有卖出记录的价格
#         if len(rec_in_days[rec_in_days.price>=today_price])>0:
#             prob_plus = 0
#         else:
#             prob_plus = 1
#         # 计算当前价格在3年内最高30%的价格中的排名占比        
#         sell_prob = 0
#         if avg_cost <=0:
#             sell_prob = 0
#             return sell_prob
#         sell_prob = len(y3_top_30_p[y3_top_30_p.price < today_price])/len(y3_top_30_p)
#         # 判断是否高于成本50%以上，（保证收益50%以上才卖出）
#         if (today_price-avg_cost)/avg_cost >= 0.5:
#             sell_prob = (today_price-avg_cost)/avg_cost + sell_prob
#             if prob_plus >0:
#                 return sell_prob
#             else:
#                 return 0
#         return 0
    # def if_buy(self, fund_code, today_date = datetime.datetime(datetime.datetime.today().year, datetime.datetime.today().month,datetime.datetime.today().day), if_verify=True, all_fund_data = None):
    #     # 获取3年内基金数据价格最低Top30%的数据
    #     start_date = (today_date - datetime.timedelta(days=365*3))
    #     duration = [start_date,today_date]
    #     past_3y_data = all_fund_data
    #     if past_3y_data is None:
    #         past_3y_data = data_operator.load_fund(fund_code, duration=duration)
    #     # 获取今日价格，若是测试则使用历史数据，否则用当日实时数据
    #     today_price = float('inf')
    #     if if_verify == True:
    #         today_price = past_3y_data[past_3y_data.date == today_date]
    #         if len(today_price.index)==0:
    #             return 0
    #         today_price = today_price.price.iloc[0]
    #     else:
    #         today_price = data_operator.get_realtime_price(fund_codes=[fund_code])
    #         if len(today_price) == 0:
    #             return 0
    #         today_price = today_price[0]
    #     # past_3y_data['price'] = past_3y_data['price'].astype(float)
    #     past_3y_data = past_3y_data[past_3y_data.date<today_date].sort_values(by=["price"])
    #     past_2y_data = past_3y_data[past_3y_data.date > (start_date+datetime.timedelta(days=364))]
    #     past_1y_data = past_3y_data[past_3y_data.date > (start_date+datetime.timedelta(days=364*2+1))]
    #     # past_2y_data = data_operator.load_fund(fund_code, duration=[(today_date-datetime.timedelta(days=364*2+1)), today_date])
    #     # past_1y_data = data_operator.load_fund(fund_code, duration=[(today_date-datetime.timedelta(days=365)), today_date])
    #     # past_2y_data['price'] = past_2y_data['price'].astype(float)
    #     # past_1y_data['price'] = past_1y_data['price'].astype(float)
    #     y3_lowest_30_p = past_3y_data[:len(past_3y_data)*3//10]
    #     y2_lowest_30_p = past_2y_data[:len(past_3y_data)*3//10]
    #     y1_lowest_30_p = past_1y_data[:len(past_1y_data)*3//10]
    #     # 计算当前价格在3年内最低30%的价格中的排名占比
    #     buy_prob = 0
    #     if not y3_lowest_30_p.empty and today_price < y3_lowest_30_p.price.iloc[-1]:
    #         buy_prob = len(y3_lowest_30_p[y3_lowest_30_p.price>today_price])/len(y3_lowest_30_p)
    #     elif not y2_lowest_30_p.empty and today_price < y2_lowest_30_p.price.iloc[-1]:
    #         buy_prob = (len(y2_lowest_30_p[y2_lowest_30_p.price>today_price])/len(y2_lowest_30_p))*0.66
    #     elif not y1_lowest_30_p.empty and today_price < y1_lowest_30_p.price.iloc[-1]:
    #         buy_prob = (len(y1_lowest_30_p[y1_lowest_30_p.price>today_price])/len(y1_lowest_30_p))*0.33
    #     else:
    #         buy_prob = 0
    #     # 读取用户资产和购买记录
    #     assets = self.user.asset
    #     records = self.user.record
    #     # if not records.empty:
    #     #     records.date = pd.to_datetime(records.date)
    #     # 判断今日价格是否比15天内的购买价格都低，如果15天以内买入过，那么就仅在当前价格比15内买入价格更低时买入。
    #     rec_in_days = records[(records.date>(today_date-datetime.timedelta(days=5))) & (records.fund_code == fund_code) & (records.buy_sell == "buy")]
    #     prob_plus = 0
    #     if len(rec_in_days[rec_in_days.price<=today_price])>0:
    #         prob_plus = 0
    #     else:
    #         prob_plus = 1
    #     target_fund = assets[assets.my_fund == fund_code]
    #     avg_cost = 0
    #     if target_fund.empty :
    #         avg_cost = 0
    #     else:
    #         avg_cost = target_fund.my_cost.iloc[0]/target_fund.my_units.iloc[0]
    #     if avg_cost == 0:
    #         return buy_prob
    #     buy_prob = buy_prob+(avg_cost-today_price)/avg_cost
    #     if prob_plus > 0:
    #         return buy_prob
    #     else:
    #         return 0             



# Test Case
# tester = Strategy(user=User(username="zehua",password = "0000", my_cash = 10000))
# print(datetime.datetime.now())
# res = tester.if_buy_accelerated(fund_list=data_operator.get_funds_list().ID.values,today_date=datetime.datetime(2020,3,1),if_verify=True)
# print(res)
# print(datetime.datetime.now(),"if buy:",res)
# operator = Operator(user=User(username="zehua",password = "0000", my_cash = 10000))
# operator.buy_at_date(fund_code="001986",date=datetime.datetime(2019,1,3),amount=100)
# operator.save_operations()
# res = tester.if_sell(fund_code="001986",today_date=datetime.datetime(2020,1,22))
# print("if sell:",res)



# test_strategy = strategy()
# a = test_strategy.if_buy("110013",'2018-12-10')
# print(a)