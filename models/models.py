import sqlalchemy as sa
from sqlalchemy import Column, String, create_engine, Float, DateTime, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import pandas as pd
import numpy as np
import os

Base = declarative_base()
TABLE_LIST = ['Fund', 'Record', 'User', 'Asset']
ENGINE_PATH = 'sqlite:///data/dbs/{}.db'

class Fund(Base):
    __tablename__ = 'fund'
    fund_code = Column(String(10), primary_key=True)
    date = Column(DateTime(timezone=False), nullable=False, default=datetime.now())
    price = Column(Float, nullable=False)
    accumulate = Column(Float, nullable=True)
    daily_rate = Column(Float, nullable=True)
    purchase_state = Column(String(10),nullable=True)
    ransom_state = Column(String(10),nullable=True)
    dividends = Column(String(10),nullable=True)

class Record(Base):
    __tablename__ = 'record'
    userid = Column(String(10),primary_key=True)
    fund_code = Column(String(10),primary_key=True)
    date = Column(DateTime(timezone=False), nullable=False, default=datetime.now(), primary_key=True)
    price = Column(Float, nullable=True)
    accumulate = Column(Float, nullable=True)
    units = Column(Float, nullable=True)
    buy_sell = Column(String(10), nullable=True)
    sold = Column(String(10), nullable=True)

class User(Base):
    __tablename__ = 'user'
    userid = Column(String(10),primary_key=True)
    username = Column(String(20), primary_key=False)
    invest_limit = Column(Float, nullable=False)
    cash_in_hand = Column(Float, nullable=False)

class Asset(Base):
    __tablename__ = 'asset'
    userid = Column(String(10),primary_key=True)
    my_units = Column(Float, nullable=False)
    my_cost = Column(Float, nullable=False)
    my_fund = Column(String(10), primary_key=True)
    fund_type = Column(String(10), primary_key=True)
    my_benefit = Column(Float, nullable=True)

class DB_operation(object):
    def __init__(self, db_path = ENGINE_PATH.format('MyFunds')):
        self.__engine__ = create_engine(db_path,echo=True)


    def create_table(self, db_path = ENGINE_PATH.format('MyFunds')):
        engine = create_engine(db_path,echo=True)
        is_created = True
        for tablename in TABLE_LIST:
            if engine.dialect.has_table(engine, tablename):
                is_created = False
        if not is_created:
            Base.metadata.bind = engine
            Base.metadata.create_all()
        return True
    
    def fund_df_to_table(self, fund_df = None, db_path = ENGINE_PATH.format('MyFunds')):
        # if not (os.path.exists(db_path) and fund_df):
        #     print("*ERROR*: the fund_df is None or the path_db not exists....")
        #     return False
        engine = create_engine(db_path, echo=True)
        fund_df.to_sql('Fund',con=engine, if_exists='append', index=False)




    def record_df_to_table(self, record_df, db_path = ""):
        pass

    def asset_df_to_table(self, asset_df, db_path = ""):
        pass

    def fund_update_one(self, ):
        pass

    def asset_update_one(self, ):    
        pass

    def user_update_one(self, ):
        pass

    def record_update_one(self, ):
        pass


# create_table()
# a = Asset(userid='dacong', my_fund='110013')
# print(type(a).__name__)


a = DB_operation()
a.create_table()
data = pd.read_pickle("/home/ritakuka/Projects/pricepredictor/data/fund_data/001013.pkl")
a.fund_df_to_table(fund_df=data)



# create_table()
# engine = create_engine(ENGINE_PATH.format("Wallet"),echo=True)
# DBSession = sessionmaker(bind=engine)

# session = DBSession()
# new_user = User(userid='ritakuka', name='ritakuka')
# session.add(instance=new_user)
# session.commit()
# session.close()

