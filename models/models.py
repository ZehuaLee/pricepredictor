import sqlalchemy as sa
from sqlalchemy import text, create_engine, Column, MetaData, and_
from sqlalchemy.dialects.sqlite import DATETIME, FLOAT, DATETIME, INTEGER, VARCHAR
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os,sys,time
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils import readconfig

Base = declarative_base()
TABLE_LIST = ['Fund', 'Record', 'User', 'Asset']
ENGINE_PATH = readconfig.config["db_engine"]+"?check_same_thread=False"

class Fund(Base):
    __tablename__ = 'Fund'
    fund_code = Column(VARCHAR(), primary_key=True)
    date = Column(DATETIME(timezone=False), nullable=False, default=datetime.now(),primary_key=True)
    price = Column(FLOAT, nullable=False)
    accumulate = Column(FLOAT, nullable=True)
    daily_rate = Column(FLOAT, nullable=True)
    purchase_state = Column(VARCHAR(),nullable=True)
    ransom_state = Column(VARCHAR(),nullable=True)
    dividends = Column(VARCHAR(),nullable=True)

# DataFrame Record:
#       userid  -->userid
#       fund_code  -->fund_code
#       date  -->date
#       price  -->price
#       accumulate  -->accumulate
#       units  -->units
#       buy_sell  -->buy_sell 
class Record(Base):
    __tablename__ = 'Record'
    userid = Column(VARCHAR,primary_key=True) # key
    fund_code = Column(VARCHAR,primary_key=True) # key
    date = Column(DATETIME(timezone=False), nullable=False, default=datetime.now(), primary_key=True)
    price = Column(FLOAT, nullable=True)
    accumulate = Column(FLOAT, nullable=False)
    units = Column(FLOAT, nullable=False)
    buy_sell = Column(VARCHAR, nullable=False)

# Dataframe User:
#         userid  -->userid
#         password  -->password
#         username  -->username
#         my_cash  -->my_cash
class User(Base):
    __tablename__ = 'User'
    userid = Column(VARCHAR(),primary_key=True)
    password = Column(VARCHAR(),primary_key=True)
    username = Column(VARCHAR(), primary_key=True)
    my_cash = Column(FLOAT, nullable=False,default=0)

# Dataframe Asset:
#         userid  -->userid
#         my_fund  -->my_fund
#         my_units  -->my_units
#         my_cost  -->my_cost
class Asset(Base):
    __tablename__ = 'Asset'
    userid = Column(VARCHAR(),primary_key=True)
    my_fund = Column(VARCHAR(), primary_key=True)
    my_units = Column(FLOAT, nullable=False)
    my_cost = Column(FLOAT, nullable=False)
    


class DB_operation(object):
    print("------------------",os.path.abspath(readconfig.config["db_file_path"]))
    def __init__(self, db_path = ENGINE_PATH.format(readconfig.config["db_file_path"])):
        print("-------------",db_path)
        self.db_path=db_path
        self.dtypedict={
            'str':VARCHAR,
            'int':INTEGER,
            'float':FLOAT,
            'datetime':DATETIME,
            'object':VARCHAR
        }
        if not os.path.exists(db_path):
            self.create_table()
        

    def create_table(self, db_path = ENGINE_PATH.format(readconfig.config["db_file_path"])):
        if not db_path:
            db_path=self.db_path
        engine = create_engine(db_path,echo=True)
        is_created = True
        for tablename in TABLE_LIST:
            if not engine.dialect.has_table(engine, tablename):
                is_created = False
        if not is_created:
            Base.metadata.bind = engine
            Base.metadata.create_all()
        return True
    
    def fund_df_to_table(self, fund_df = None, db_path = ""):
        # if not (os.path.exists(db_path) and fund_df):
        #     print("*ERROR*: the fund_df is None or the path_db not exists....")
        #     return False
        if not db_path:
            db_path = self.db_path
        engine = create_engine(db_path, echo=True)
        fund_df.to_sql('Fund',con=engine, if_exists='append',index=False, dtype=self.dtypedict, chunksize = 10000)

    def sql_query(self, sql):
        engine = create_engine(self.db_path)
        df = pd.read_sql(sql=sql, con=engine)
        return df

    # fund_code, date   
    def fund_update(self, data_df):
        engine = create_engine(self.db_path)
        Session=sessionmaker(bind=engine)
        session=Session()
        for i in range(len(data_df)):
            r1 = session.query(Fund).filter(and_(Fund.fund_code==data_df["fund_code"].iloc[i],Fund.date==data_df["date"].iloc[i])).first()
            # print(r1, type(r1))
            if r1 is None:
                fund = Fund(fund_code=data_df.fund_code.iloc[i],date=data_df.date.iloc[i],price=data_df.price.iloc[i],accumulate=data_df.accumulate.iloc[i],daily_rate=data_df.daily_rate.iloc[i],purchase_state=data_df.purchase_state.iloc[i],ransom_state=data_df.ransom_state.iloc[i],dividends=data_df.dividends.iloc[i])
                session.add(fund)
            else:
                r1.price=data_df.price.iloc[i]
                r1.accumulate=data_df.accumulate.iloc[i]
                r1.daily_rate=data_df.daily_rate.iloc[i]
                r1.purchase_state=data_df.purchase_state.iloc[i]
                r1.ransom_state=data_df.ransom_state.iloc[i]
                r1.dividends=data_df.dividends.iloc[i]
        session.commit()

    # userid, my_fund
    def asset_update(self, asset_df):
        engine = create_engine(self.db_path)
        Session=sessionmaker(bind=engine)
        session=Session()
        for i in range(len(asset_df)):
            r1 = session.query(Asset).filter(and_(Asset.my_fund==asset_df.my_fund.iloc[i],Asset.userid==asset_df.userid.iloc[i])).first()
            if r1 is None:
                asset = Asset(userid=asset_df.userid.iloc[i],my_fund=asset_df.my_fund.iloc[i],my_units=asset_df.my_units.iloc[i],my_cost=asset_df.my_cost.iloc[i])
                session.add(asset)
            else:
                r1.my_units = asset_df.my_units.iloc[i]
                r1.my_cost = asset_df.my_cost.iloc[i]
                if r1.my_units == 0:
                    session.delete(r1)
        session.commit()

    # userid, password
    def user_update(self, user_df):
        engine = create_engine(self.db_path)
        Session = sessionmaker(bind=engine)
        session = Session()
        for i in range(len(user_df)):
            r1 = session.query(User).filter(User.userid==user_df.userid.iloc[i]).first()
            if not r1 :
                user = User(userid=user_df.userid.iloc[i],password=user_df.password.iloc[i],username=user_df.username.iloc[i],my_cash=user_df.my_cash.iloc[i])
                session.add(user)
            else:
                r1.password = user_df.password.iloc[i]
                r1.username=user_df.username.iloc[i]
                r1.my_cash=user_df.my_cash.iloc[i]
        flag = True
        while flag:
            try:
                session.commit()
                flag = False
                print("commit succeed.")
            except:
                flag = True
                time.sleep(0.1)

    # userid, fund_code, date
    def record_update(self, record_df):
        engine = create_engine(self.db_path)
        Session = sessionmaker(bind=engine)
        session = Session()
        for i in range(len(record_df)):
            r1 = session.query(Record).filter(and_(Record.userid==record_df.userid.iloc[i], Record.fund_code==record_df.fund_code.iloc[i], Record.date==record_df.date.iloc[i])).first()
            if r1 == None:
                record = Record(userid=record_df.userid.iloc[i], fund_code=record_df.fund_code.iloc[i], date=record_df.date.iloc[i], price=record_df.price.iloc[i], accumulate=record_df.accumulate.iloc[i], units=record_df.units.iloc[i], buy_sell = record_df.buy_sell.iloc[i])
                session.add(record)
            else:
                r1.price = record_df.price.iloc[i]
                r1.accumulate = record_df.accumulate.iloc[i]
                r1.units = record_df.units.iloc[i]
                r1.buy_sell = record_df.buy_sell.iloc[i]
        session.commit()

    def delete_asset_by_user(self, userid=""):
        engine = create_engine(self.db_path)
        Session=sessionmaker(bind=engine)
        session=Session()
        try:
            assets = session.query(Asset).filter(Asset.userid==userid).all()
            if len(assets)==0:
                return True
            for asset in assets:
                session.delete(asset)
        except:
            print("Error fund in deleting assets by userid")
        else:    
            flag = True
            while flag:
                try:
                    session.commit()
                    flag = False
                    print("commit succeed.")
                except:
                    flag = True
                    time.sleep(0.5)
    
    def delete_record_by_user(self, userid = ""):
        engine = create_engine(self.db_path)
        Session=sessionmaker(bind=engine)
        session=Session()
        try:
            records = session.query(Record).filter(Record.userid == userid).all()
            if len(records)==0:
                return True
            for record in records:
                session.delete(record)
        except:
            print("Error fund in deleting records by userid")
        else:
            session.commit()

    def delete_user_by_id(self, userid = ""):
        engine = create_engine(self.db_path)
        Session=sessionmaker(bind=engine)
        session=Session()
        try:
            users = session.query(User).filter(User.userid == userid).all()
            if len(users)==0:
                return True
            for user in users:
                session.delete(user)
        except Exception:
            print("Error fund in deleting users by userid")
        else:
            session.commit()
    def delete_users_by_ids(self, userids = []):
        engine = create_engine(self.db_path)
        Session=sessionmaker(bind=engine)
        session=Session()
        try:
            users = session.query(User).filter(User.userid.in_(userids)).all()
            if len(users)==0:
                return True
            for user in users:
                session.delete(user)
        except:
            print("Error fund in deleting users by userids")
        else:
            session.commit()
db_operator = DB_operation()


# create_table()
# a = Asset(userid='dacong', my_fund='110013')
# print(type(a).__name__)


# create table
# a = DB_operation()
# a.create_table()

# create engine
# engine = create_engine(ENGINE_PATH.format("all"))
# Session=sessionmaker(bind=engine)
# session=Session()

# pkl to database
# data = pd.read_pickle(os.path.join(readconfig.config["funds_folder"]+"funds","020021.pkl"))
# new_data = data.rename(columns={"purchuse_state":"purchase_state"})
# print("the first line:", new_data.iloc[0])
# new_data["date"]=pd.to_datetime(new_data.date)
# data1=new_data
# # db_operator.fund_df_to_table(fund_df=data1)
# db_operator.fund_update(data_df = data1)


# update time
# data2=data1[data1.date==datetime(2019,9,6)]
# data2.price.iloc[0]=0.22222222
# data2.date.iloc[0]=datetime(2019,9,6)+timedelta(days=1)
# database update
# a.fund_update(data_df=data2)

# sql_query by sql string
# SQLString = """select * from Fund where date='2019-09-06 00:00:00.000000'"""
# res = a.sql_query(sql=SQLString)

# create_table()
# engine = create_engine(ENGINE_PATH.format("Wallet"),echo=True)
# DBSession = sessionmaker(bind=engine)

# session = DBSession()
# new_user = User(userid='ritakuka', name='ritakuka')
# session.add(instance=new_user)
# session.commit()
# session.close()

