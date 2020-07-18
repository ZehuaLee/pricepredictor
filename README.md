# Price prediction platform for fund in Chinese market.
This platform provides a suite of tools to help users achieve following goals. The whole platform could be divided into 3 function parts
## Function parts
1. fund data operator
    - get fund data both historical and the latest.
    - automatically parsing fund data and save to db
    - db file/type can be set in config.json
2. stragegy management
    - this platform could be connected to a flask web app
    - strategy could be set in config file. 
    - strategy verification platform/tools
3. asset management
    - personal virual cash/asset management, multi-users supported.
4. simulate purchasing/selling fund operations.
    - simulate purchasing/selling fund
    - operation record is kept in db.

## Project architecture introduction
### **utils**
This package provides a suite of tool including scapping fund data, clearing data,  operating data and wrapped CRUD operations.

1. **get_fund.py**   -> Data_operator class defined a set of functions to scraping data from [eastmoney](http://fund.eastmoney.com/fund.html)
    - get_one_fund() -> scrapping one fund at a certain date slot
    - read_multiple() -> a subprocess called in multi-thread/multi-process scrapping
    - get_funds_multi_process() -> accelerate scrapping data by multi-processes.
    - get_funds_multi_thread() -> accelerate scrapping data by multi-threads.
    - get_funds() -> scrapping fund by single process/single thread.
    - load_fund() -> select target fund data from database.
    - load_funds() -> select multiple target funds data from database.
    - load_asset() -> laod user's asset from db.
    - load_record() -> load purchasing/selling record data from db.
    - update_fund() -> update the fund data,crud on fund in db.
    - get_fund_list() -> read fund list.
    - get_realtime_price() -> get the real time fund price.  

2. **operatefund.py**   -> Operator class defines a simulation of buy/sell operations.
    - buy_at_date() -> purchase a certain amount of fund at the price of a specific date.
    - sell_at_date() -> sell a certain amount of fund at the price of a specific date.
    - sell_fund_all() -> sell all fund shares hold in hand.
    - get_asset_value() -> calculate value of fund hold in hand.
3. **readconfg.py** -> read config file.
4. **playground.py** -> for unit test and trials.
5. **wechat.py** -> for future wechat notification function.
### strategies 
Some samples of strategies, jupyter note or stragegy py file.
1. fixedrule.py -> define some fixed rules to decide when to buy/sell. 
2. SingleLSTM.ipynb -> LSTM model on predicting one fund price.
3. verify.py -> back test/verify the strategy.
4. AffinityCluster.ipynb -> Apply cluster algorithm on funds.
### **models** 
Define database schema and basice data operations.
1. Fund
   - fund_code
   - date
   - price
   - accumulate
   - daily_rate
   - pruchase_state
   - ransom_state
   - dividends
2. User
    - userid
    - password
    - username
    - my_cash
3. Record
    - userid
    - fund_code
    - date
    - price
    - accumulate
    - units
    - buy_sell
4. Asset
    - userid
    - my_fund
    - my_units
    - my_cost
### frontend
User asset/Record management page, under consturction. vue.js.
### backend
flask supported
### others
config.json

## platform for analyst to create algorithm in strategies
write your strategies in ./strategies and config it in config.json
## please check the requirements.txt to make sure your env is right
Recomand you to use conda to install or virtualenv is also a good choice.
