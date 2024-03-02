import pytest
import datetime
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from models.financial_transaction import FinancialTransactionTest
from db_utils.db_functions import query, save_to_db
from classes.db_credentials import DBCredentials

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DATABASE = os.getenv('DB_DATABASE')

@pytest.fixture()
def db_setup():
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}')

    # Define your SQL query
    query = text("""
        DROP TABLE IF EXISTS financial_transactions_test;
        CREATE TABLE financial_transactions_test (
            id VARCHAR,
            transaction_type VARCHAR,
            amount FLOAT,
            date TIMESTAMPTZ,
            description VARCHAR,
            category VARCHAR
        );

        INSERT INTO financial_transactions_test VALUES
        (1, 'debit', 100.0, '2024-01-01', 'Sample Description', 'Sample Category');
    """)

    # Execute the query and fetch all results
    with engine.connect() as connection:
        result = connection.execute(query)

    yield result

    # Define your SQL query
    query = text("""
        DROP TABLE IF EXISTS financial_transactions_test;
    """)

    # Execute the query and fetch all results
    with engine.connect() as connection:
        result = connection.execute(query)    


def test_query(db_setup):
    db_creds = DBCredentials(
        host = 'aws-0-us-west-1.pooler.supabase.com',
        port = '5432',
        user = 'postgres.jnkydlefgollpcwqjbps',
        password = 'IuHKTkPi8radGv7o',
        database = 'postgres'
    )

    res = query(
        sql = "SELECT * FROM financial_transactions_test WHERE id = '1'",
        db_creds = db_creds,
    )

    assert res == [('1', 'debit', 100.0, 
                    datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.timezone.utc), 
                    'Sample Description', 
                    'Sample Category')]


def test_save_to_db(db_setup):

    db_creds = DBCredentials(
        host = 'aws-0-us-west-1.pooler.supabase.com',
        port = '5432',
        user = 'postgres.jnkydlefgollpcwqjbps',
        password = 'IuHKTkPi8radGv7o',
        database = 'postgres'
    )

    sample_data = {
        'id': '2',
        'transaction_type': 'credit',
        'amount': 200.0,
        'date': '2024-03-01',
        'description': 'Groceries',
        'category': 'Food'
    }
    
    save_to_db(model = FinancialTransactionTest, data_json = sample_data, db_creds = db_creds)

    records = query(
        sql = 'SELECT * FROM financial_transactions_test', 
        db_creds = db_creds,
    )

    assert records == [

        ('1', 'debit', 100.0, 
        datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.timezone.utc), 
        'Sample Description', 
        'Sample Category'),

        ('2', 'credit', 200.0, 
        datetime.datetime(2024, 3, 1, 0, 0, tzinfo=datetime.timezone.utc), 
        'Groceries', 
        'Food'),
        
    ]

    
