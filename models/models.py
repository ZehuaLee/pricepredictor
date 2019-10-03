import sqlalchemy as sa
from sqlalchemy import Column, String, create_engine, Float, DateTime, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()
TABLE_LIST = ['Fund', 'Record', 'User', 'Wallet']
ENGINE_PATH = 'sqlite:///data/dbs/wallet.db'

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
    __tablename__ = 'User'
    userid = Column(String(10),primary_key=True)
    name = Column(String(20), primary_key=False)

class Wallet(Base):
    __tablename__ = 'wallet'
    userid = Column(String(10),primary_key=True)
    my_units = Column(Float, nullable=False)
    my_cost = Column(Float, nullable=False)
    my_fund = Column(String(10), primary_key=True)

class DB_operation(object):
    def create_table(self, table_name='Wallet', db_path = ENGINE_PATH):
        if table_name not in TABLE_LIST:
            return False, "schema not exists"
        engine = create_engine(db_path,echo=True)
        if not engine.dialect.has_table(engine, table_name):
            Base.metadata.bind = engine
            Base.metadata.create_all()
    
    # def add_data(self, table_name='Wallet', )

a = Wallet(userid='dacong', my_fund='110013')
print(type(a).__name__)




# create_table()

# DBSession = sessionmaker(bind=engine)

# session = DBSession()
# new_user = User(userid='ritakuka', name='ritakuka')
# session.add(instance=new_user)
# session.commit()
# session.close()
