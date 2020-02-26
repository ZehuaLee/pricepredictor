import os
import sys
sys.path.append(os.path.abspath(os.path.join(__file__, "../../utils")))
from getfund import GetData
from operatefund import Operation
import pandas as pd
import fixedrule
import datetime

# class verify(object):
#     def __init__(self,fund_operation=None,duration=[]):
#         self.verify_duration = duration
#         self.profits = 0
#         self.my_operations = Operation()
#         self.get_data = GetData()
#         self.strategy_path = 
    
#     def verify_
# verify = verify()    
getf = GetData()
fundcode = "110012"

test_all_data = getf.load_fund(fund_code=fundcode, duration=["2015-09-07", "2018-09-15"])
test_all_data['price'] = test_all_data['price'].astype(float)
ope = Operation(total_data=None)
fixstr = fixedrule.strategy(fund_codes=[fundcode])
result = ope.verify_strategy_on_single_fund(mystrategy = fixstr, fund_code=fundcode,test_data = test_all_data)
print("result",result)

# ope.buy_at_date("110013", str(datetime.datetime.now().date()), amount=10, if_value=True)
# print(ope.records.values)