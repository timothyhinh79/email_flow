from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class FinancialTransaction(Base):
    __tablename__ = 'financial_transactions'
    __table_args__ = {'schema': 'public'}

    id = Column(String, primary_key=True)
    transaction_type = Column(String)
    amount = Column(Float)
    date = Column(DateTime)
    description = Column(String)
    category = Column(String)

class FinancialTransactionTest(Base):
    __tablename__ = 'financial_transactions_test'
    __table_args__ = {'schema': 'public'}

    id = Column(String, primary_key=True)
    transaction_type = Column(String)
    amount = Column(Float)
    date = Column(DateTime)
    description = Column(String)
    category = Column(String)