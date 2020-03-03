from utils.getfund import data_operator
import pandas as pd
from utils.operatefund import Operator, User
from models.models import db_operator
from utils.readconfig import config
import datetime
import os
from multiprocessing import Process, Queue, Pool
# from strategies.fixed import Strategy
exec("from strategies.{} import Strategy".format(config["strategy_file"]))
from concurrent.futures import ThreadPoolExecutor
from strategies.fixed import Strategy
class Runner(object):
    def __init__(self, user = User(username="Runner", password="1234", my_cash=5000)):
        self.user = user
        

    def save_latest_price(self, today=datetime.datetime.today(), timedelta = 10):
        today_date = datetime.datetime(today.year, today.month, today.day)
        fund_list = data_operator.get_funds_list().ID.values
        today_fund = data_operator.get_funds_multi_thread(fund_list=fund_list, duration=[today_date-datetime.timedelta(timedelta), today_date], entry_per_page=20)
        data_operator.update_fund(today_fund)


    def get_funds_to_buy(self, fund_list = data_operator.get_funds_list().ID.values):
        # real_time_price = data_operator.get_realtime_price(fund_list)
        today = datetime.datetime.today()
        self.save_latest_price()
        my_strategy = Strategy(user=self.user)
        prob_to_buy = my_strategy.if_buy_accelerated(fund_list=fund_list, today_date=today, if_verify=False, all_data_fund=None)
        # prob_to_sell = my_strategy.if_sell_accelerated_2(fund_list=self.user.asset.my_fund.values, today_date=today, if_verify=True, all_data_fund=None)
        if len(prob_to_buy)>0:
            for code in prob_to_buy.index:
                print("buy: ", code, "prob: ", prob_to_buy[code])
                # tester_operator.buy_at_date(fund_code=code, date=today, amount=30, if_value=True)
        # if len(prob_to_sell)>0:
        #     for code in prob_to_sell.index:
        #         print("sell: ", code, "prob: ", prob_to_sell[code])
        #         # tester_operator.sell_fund_all(fund_code=code,date=today)
        return prob_to_buy

    def run(self):
        pass


class Verify(object):
    def __init__(self, tester_name = "tester", password = "0000", my_cash = 2000):
        self.tester = User(username="tester", password="0000", my_cash=my_cash)
        # self.tester.update_my_cash(my_cash)
        # self.verify_interval = config["verify_interval"]
        # self.verify_all_funds = False
        # self.funds_to_verify=[]


    def verify_one_fund(self, tester = None, fund_code="", duration=[(datetime.datetime(2012,1,31)+datetime.timedelta(365*3)),(datetime.datetime(2012,1,31)+datetime.timedelta(365*8))], principle = 3000):
        if tester is None:
            tester = User(username="tester_"+fund_code,password="0000",my_cash=principle)
        test_funds = data_operator.load_fund(fund_code=fund_code,duration = duration)
        my_operator = Operator(tester)
        my_strategy = Strategy(tester)
        asset_before = my_operator.get_asset_value(date=duration[0], contains_cash=True,realtimeprice=False)
        for i in range(len(test_funds)):
            buy_prob = my_strategy.if_buy(fund_code,today_date=test_funds.date.iloc[i],if_verify=True)
            sell_prob = my_strategy.if_sell(fund_code=fund_code, today_date=test_funds.date.iloc[i], if_verify=True)
            # print(test_funds.date.iloc[i],self.tester.my_cash, my_operator.user.my_cash, my_strategy.user.my_cash, buy_prob, sell_prob)
            res = buy_prob if buy_prob>sell_prob else sell_prob
            if buy_prob > sell_prob and res > 0.7:
                my_operator.buy_at_date(fund_code=fund_code,date=test_funds.date.iloc[i],amount=config["amount"],if_value=config["if_value"])
                # my_operator.off_work(date=test_funds.date.iloc[i])
            elif buy_prob < sell_prob and res >0.7:
                my_operator.sell_at_date(fund_code=fund_code, date=test_funds.date.iloc[i],amount=config["amount"], if_value=config["if_value"])
                # my_operator.off_work(date=test_funds.date.iloc[i])
            else:
                None
        asset_after = my_operator.get_asset_value(date=duration[1],contains_cash=True, realtimeprice=False)
        tester.delete_from_db()
        print(fund_code,[asset_before, asset_after, (asset_after-asset_before)/asset_before, (asset_after-asset_before)/(asset_before*(duration[1]-duration[0]).days/365)])
        return [asset_before, asset_after, (asset_after-asset_before)/asset_before, (asset_after-asset_before)/(asset_before*(duration[1]-duration[0]).days/365)]

    def verify_funds(self, duration=[datetime.datetime(2015,1,31),(datetime.datetime(2012,1,31)+datetime.timedelta(365*8))]):
        fund_in_db = db_operator.sql_query(sql = "SELECT distinct fund_code from Fund").fund_code.values
        fund_list = data_operator.get_funds_list()
        fund_list = fund_list[fund_list.ID.isin(fund_in_db)]
        principal_fund = 1000
        self.tester = User(username="tester", password="0000", my_cash=principal_fund)
        predict_results = pd.DataFrame({"fund_code":[],"fund_name":[],"principal":[],"init_asset":[],"result":[],"gain_rate":[], "yearly_gain_rate":[]})
        for i in range(len(fund_list)):
            res = self.verify_one_fund(fund_code=fund_list.ID.iloc[i],duration=duration)
            predict_results.append([[fund_list.ID.iloc[i],principal_fund, res[0],res[1],res[2],res[3]]])
            print("fund_code:"+str(fund_list.ID.iloc[i]),"fund_name:"+fund_list.Name.iloc[i],"init:"+str(res[0]),"result:"+str(res[1]),"gain_rate:"+str(res[2]),"yearly_gain_rate:"+str(res[3]))
        predict_results.to_csv(config["output_path"])
    
    def verify_funds_multithread(self, duration = [datetime.datetime(2015,1,31), (datetime.datetime(2012,1,31)+datetime.timedelta(365*8))], principal = 3000):
        print("job started: ", datetime.datetime.now())
        fund_in_db = db_operator.sql_query(sql = "SELECT distinct fund_code from Fund").fund_code.values
        fund_list = data_operator.get_funds_list()
        fund_list = fund_list[fund_list.ID.isin(fund_in_db)]
        predict_result = {}
        predict_results = pd.DataFrame({"fund_code":[],"fund_name":[],"principal":[],"init_asset":[],"result":[],"gain_rate":[], "yearly_gain_rate":[]})
        with ThreadPoolExecutor(config["threads"]) as executor:
            for i in range(len(fund_list)):
                # tester = User(username="test_"+fund_list.ID.iloc[i], password="0000", my_cash=3000)
                print(fund_list.ID.iloc[i], "started @", datetime.datetime.now())
                r1 = executor.submit(self.verify_one_fund, None, fund_list.ID.iloc[i],duration, principal)
                predict_result[fund_list.ID.iloc[i]] = r1
                print(fund_list.ID.iloc[i], "ended @", datetime.datetime.now())
        for (x, y) in predict_result.items():
            res = y.result()
            new_record = {}
            new_record["fund_code"] = x
            new_record["fund_name"] = fund_list[fund_list.ID == x].Name.iloc[0]
            new_record["principal"] = principal
            new_record["init_asset"] = res[0]
            new_record["result"] = res[1]
            new_record["gain_rate"] = res[2]
            new_record["yearly_gain_rate"] = res[3]
            predict_results.append(new_record,ignore_index=True)
            print("fund_code:"+new_record["fund_code"],"fund_name:"+new_record["fund_name"],"init:"+str(res[0]),"result:"+str(res[1]),"gain_rate:"+str(res[2]),"yearly_gain_rate:"+str(res[3]))
        if os.path.exists(config["output_path"]):
            os.remove(config["output_path"])
        predict_results.to_csv(config["output_path"])

    def verify_funds_multiprocesses(self, duration=[datetime.datetime(2015,1,31),datetime.datetime(2020,1,28)], principal = 3000):
        p = Pool(config["ps_num"])
        print("job started: ", datetime.datetime.now())
        fund_in_db = db_operator.sql_query(sql = "SELECT distinct fund_code from Fund").fund_code.values
        fund_list = data_operator.get_funds_list()
        fund_list = fund_list[fund_list.ID.isin(fund_in_db)]
        predict_result = {}
        predict_results = pd.DataFrame({"fund_code":[],"fund_name":[],"principal":[],"init_asset":[],"result":[],"gain_rate":[], "yearly_gain_rate":[]})
        for i in range(len(fund_list)):
            a = p.apply_async(func = self.verify_one_fund, args=(None, fund_list.ID.iloc[i],duration, principal))
            predict_result[fund_list.ID.iloc[i]] = a
        p.close()
        p.join()
        print("***** All jobs were thrown *****")
        for (x, y) in predict_result.items():
            res = y.get()
            new_record = {}
            new_record["fund_code"] = x
            new_record["fund_name"] = fund_list[fund_list.ID == x].Name.iloc[0]
            new_record["principal"] = principal
            new_record["init_asset"] = res[0]
            new_record["result"] = res[1]
            new_record["gain_rate"] = res[2]
            new_record["yearly_gain_rate"] = res[3]
            predict_results.append(new_record,ignore_index=True)
            print("fund_code:"+new_record["fund_code"],"fund_name:"+new_record["fund_name"],"init:"+str(res[0]),"result:"+str(res[1]),"gain_rate:"+str(res[2]),"yearly_gain_rate:"+str(res[3]))
        if os.path.exists(config["output_path"]):
            os.remove(config["output_path"])
        predict_results.to_csv(config["output_path"])

    def select_best_to_buy(self, tester = None, fund_list=None, date = datetime.datetime(2019,1,2)):
        print("buy select started:",datetime.datetime.now())
        if tester is None:
            tester = self.tester
        if fund_list is None:
            fund_in_db = db_operator.sql_query(sql = "SELECT distinct fund_code from Fund").fund_code.values
            fund_list = data_operator.get_funds_list()
            fund_list = fund_list[fund_list.ID.isin(fund_in_db)]
        best_fund = ["",0]
        my_strategy = Strategy(tester)
        # test_all_funds_data = data_operator.load_funds(fund_list.ID.values,duration=[date-datetime.timedelta(days = 365*3), date])
        t1 = datetime.datetime.now()
        for fund_code in fund_list.ID.values:
            # all_fund_for_today = test_all_funds_data[test_all_funds_data.fund_code == fund_code]
            best_prob = my_strategy.if_buy(fund_code, date, True, all_fund_data=None)
            # print("+",fund_code, best_prob, date)
            if best_prob> best_fund[1]:
                best_fund[0] = fund_code
                best_fund[1] = best_prob
        print(datetime.datetime.now()-t1, "used.")
        print("+", best_fund, date, datetime.datetime.now())
        return best_fund

    def select_best_to_sell(self, tester = None, fund_list=None, date = datetime.datetime(2019,1,2)):
        print("sell select started:",datetime.datetime.now())
        if tester is None:
            tester = self.tester
        if fund_list is None:
            fund_in_db = db_operator.sql_query(sql = "SELECT distinct fund_code from Fund").fund_code.values
            fund_list = data_operator.get_funds_list()
            fund_list = fund_list[fund_list.ID.isin(fund_in_db)]
        best_fund = ["",0]
        # test_all_funds_data = data_operator.load_funds(fund_list.ID.values,duration=[date-datetime.timedelta(days = 365*3), date])
        my_strategy = Strategy(tester)
        t1 = datetime.datetime.now()
        for fund_code in fund_list.ID.values:
            # all_fund_for_today = test_all_funds_data[test_all_funds_data.fund_code == fund_code]
            best_prob = my_strategy.if_sell(fund_code, date, True, all_data_fund=None)
            if best_prob> best_fund[1]:
                best_fund[0] = fund_code
                best_fund[1] = best_prob
        print(datetime.datetime.now()-t1, "used.")
        print("-", best_fund, date, datetime.datetime.now())
        return best_fund

    def verify_comp(self, duration = [datetime.datetime(2015,1,31),datetime.datetime(2020,1,28)], principal = 3000):
        fund_in_db = db_operator.sql_query(sql = "SELECT distinct fund_code from Fund").fund_code.values
        fund_list = data_operator.get_funds_list()
        fund_list = fund_list[fund_list.ID.isin(fund_in_db)]
        tester = User(username = "test_"+str(datetime.datetime.now().microsecond), password="0000",my_cash=principal)
        tester_operator = Operator(user=tester)
        asset_before = tester_operator.get_asset_value(date = duration[0], contains_cash=True, realtimeprice=False)
        for i in range((duration[1]-duration[0]).days+1):
            today = duration[0]+datetime.timedelta(days=i)
            fund_sell_prob = self.select_best_to_sell(tester = tester,fund_list=fund_list,date = today)
            fund_buy_prob = self.select_best_to_buy(tester = tester, fund_list = fund_list, date = today)
            if fund_sell_prob[1] > 0.7:
                tester_operator.sell_at_date(fund_code=fund_sell_prob[0],date=today, amount=100, if_value=True)
            if fund_buy_prob[1] > 0.7:
                tester_operator.buy_at_date(fund_code=fund_buy_prob[0],date=today, amount=100, if_value=True)
                print(tester_operator.user.asset)
        asset_after = tester_operator.get_asset_value(duration[1],contains_cash=True,realtimeprice=False)
        print(tester.asset, tester.my_cash)
        gain = asset_after-asset_before
        gain_rate = (asset_after-asset_before)/asset_before
        yearly_gain_rate = gain_rate/((duration[1]-duration[0]).days/365)
        return [asset_before, asset_after,gain, gain_rate, yearly_gain_rate]
    
    def verify_comp_accelerated(self, duration = [datetime.datetime(2015,1,31),datetime.datetime(2020,1,28)], principal = 3000):
        tester = User(username = "test_"+str(datetime.datetime.now().microsecond), password="0000",my_cash=principal)
        tester_operator = Operator(user=tester)
        asset_before = tester_operator.get_asset_value(date = duration[0], contains_cash=True, realtimeprice=False)
        fund_list = db_operator.sql_query(sql = "SELECT distinct fund_code from Fund").fund_code.values
        for i in range((duration[1]-duration[0]).days+1):
            today = duration[0]+datetime.timedelta(days=i)
            my_strategy = Strategy(tester)
            prob_to_buy = my_strategy.if_buy_accelerated(fund_list=fund_list, today_date=today, if_verify=True, all_data_fund=None)
            prob_to_sell = my_strategy.if_sell_accelerated_2(fund_list=tester.asset.my_fund.values, today_date=today, if_verify=True, all_data_fund=None)
            if len(prob_to_buy)>0:
                for code in prob_to_buy.index:
                    print("buy: ", code)
                    tester_operator.buy_at_date(fund_code=code,date=today, amount=30, if_value=True)
            if len(prob_to_sell)>0:
                for code in prob_to_sell.index:
                    print("sell: ", code)
                    tester_operator.sell_fund_all(fund_code=code,date=today)
            asset_after = tester_operator.get_asset_value(date=today,contains_cash=True,realtimeprice=False)
            print(today, "  asset_before:", asset_before, "  asset_after:",asset_after,"  cash:", tester.my_cash)
        asset_after = tester_operator.get_asset_value(duration[1],contains_cash=True,realtimeprice=False)
        gain = asset_after-asset_before
        gain_rate = (asset_after-asset_before)/asset_before
        yearly_gain_rate = gain_rate/((duration[1]-duration[0]).days/365)
        print(30,30)
        tester_operator.save_operations()
        return [asset_before, asset_after,gain, gain_rate, yearly_gain_rate]

    def verify_comp_multithread(self, dutation = [datetime.datetime(2015,1,31), datetime.datetime(2020,1,28)]):
        pass

    def start_verify(self):
        pass
        

verifier = Verify()
runner = Runner()
# result = verifier.verify_one_fund(fund_code="110031",duration=[datetime.datetime(2015,1,31), datetime.datetime(2020,1,28)])
# print(result)
#res1 = verifier.select_best_to_buy()
# res2 = verifier.select_best_to_sell(date=datetime.datetime(2020,1,28))
# print(res2)
# verifier.verify_funds_multithread(duration=[datetime.datetime(2015,1,31), datetime.datetime(2020,1,28)])
# verifier.verify_funds_multiprocesses(duration=[datetime.datetime(2015,1,31), datetime.datetime(2020,1,28)],principal=3000)

t1 = datetime.datetime.now()
res = runner.get_funds_to_buy()
print("time spent: ")
print(datetime.datetime.now()-t1)
# res = verifier.verify_comp_accelerated(duration=[datetime.datetime(2015,1,4), datetime.datetime(2016,1,1)],principal=5000)
print(res)
# tester = User(username="hhhh", password="0000", my_cash=3000)
# op = Operator(user=tester)
# op.buy_at_date(fund_code="110013", date = datetime.datetime(2015,12,10),amount = 100, if_value=True)
# print(op.user.asset, tester.asset)



