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
                message_id VARCHAR,
                transaction_type VARCHAR,
                amount FLOAT,
                transaction_date TIMESTAMPTZ,
                description VARCHAR,
                category VARCHAR,
                updated_at TIMESTAMPTZ
        );

        INSERT INTO financial_transactions_test VALUES
        (1, 'message_id_1', 'debit', 100.0, '2024-01-01 00:00:05-08', 'Sample Description', 'Sample Category', '2024-01-02 00:00:12-08');
    """)

    # Execute the query and fetch all results
    with engine.connect() as connection:
        result = connection.execute(query)
        
    # breakpoint()
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
        host = DB_HOST,
        port = DB_PORT,
        user = DB_USER,
        password = DB_PASSWORD,
        database = DB_DATABASE
    )
    
    res = query(
        sql = "SELECT * FROM financial_transactions_test WHERE id = '1'",
        db_creds = db_creds,
    )

    assert res == [('1', 'message_id_1', 'debit', 100.0, 
                    datetime.datetime(2024, 1, 1, 8, 0, 5, tzinfo=datetime.timezone.utc), 
                    'Sample Description', 
                    'Sample Category',
                    datetime.datetime(2024, 1, 2, 8, 0, 12, tzinfo=datetime.timezone.utc))]


def test_save_to_db(db_setup):

    db_creds = DBCredentials(
        host = DB_HOST,
        port = DB_PORT,
        user = DB_USER,
        password = DB_PASSWORD,
        database = DB_DATABASE
    )

    sample_data = {
        'id': '2',
        'message_id': 'message_id_2',
        'transaction_type': 'credit',
        'amount': 200.0,
        'transaction_date': datetime.datetime(2024, 2, 1, 0, 0, tzinfo=datetime.timezone.utc),
        'description': 'Groceries',
        'category': 'Food',
        'updated_at': datetime.datetime(2024, 2, 1, 0, 0, tzinfo=datetime.timezone.utc)
    }
    
    save_to_db(model = FinancialTransactionTest, data_json = sample_data, db_creds = db_creds)

    records = query(
        sql = 'SELECT * FROM financial_transactions_test', 
        db_creds = db_creds,
    )

    assert records == [

        ('1', 'message_id_1', 'debit', 100.0, 
        datetime.datetime(2024, 1, 1, 8, 0, 5, tzinfo=datetime.timezone.utc), 
        'Sample Description', 
        'Sample Category',
        datetime.datetime(2024, 1, 2, 8, 0, 12, tzinfo=datetime.timezone.utc)),

        ('2', 'message_id_2', 'credit', 200.0, 
        datetime.datetime(2024, 2, 1, 0, 0, tzinfo=datetime.timezone.utc), 
        'Groceries', 
        'Food',
        datetime.datetime(2024, 2, 1, 0, 0, tzinfo=datetime.timezone.utc)),
        
    ]

    
