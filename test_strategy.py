import os, sys
sys.path.append(os.path.dirname(__file__))
from utils.operatefund import Operator,User
from utils.getfund import data_operator
from utils.readconfig import config
import datetime
exec("from strategies.{} import Strategy".format(config["strategy_file"]))
class Tester(object):
    def __init__(self, user = User(username="Tester", password="0000", my_cash=10000)):
        self.user = user
        self.operator = Operator(self.user)
        self.strategy = Strategy()

    def test_strategy_on_one_fund(self, fund_code="", test_data=None):
        if not fund_code:
            return [], "the fund code is not specified"
        if not test_data:
            test_data = data_operator.load_fund(fund_code=fund_code)
        pass
        # return [gain, yearly_gain, yearly_gain_rate, cost]

fds_list = data_operator.get_funds_list()
a = data_operator.get_funds_multi_thread(fund_list=fds_list.ID.values, duration=[datetime.datetime(2019,12,10), datetime.datetime(2020,2,27)])
data_operator.update_fund(df_fund_data=a)

