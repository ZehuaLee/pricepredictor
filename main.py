from utils.getfund import *

get_data = GetData()
fund = get_data.get_one_fund("110013")
print(fund)