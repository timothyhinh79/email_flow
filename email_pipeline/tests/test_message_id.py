import pytest
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

from src.database.models.message_id import MessageIDsTest
from src.entities.db_credentials import DBCredentials

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
        DROP TABLE IF EXISTS message_ids_test;
        CREATE TABLE message_ids_test (
                id VARCHAR primary key,
                message_id VARCHAR,
                updated_at TIMESTAMPTZ
        );

        INSERT INTO message_ids_test VALUES
        ('1', 'id_1', '2024-01-01 00:00:05-08') ,
        ('2', 'id_2', '2024-02-01 00:00:05-08')
    """)

    # Execute the query and fetch all results
    with engine.connect() as connection:
        result = connection.execute(query)
        
    # breakpoint()
    yield result

    # Define your SQL query
    query = text("""
        DROP TABLE IF EXISTS message_ids_test;
    """)

    # Execute the query and fetch all results
    with engine.connect() as connection:
        result = connection.execute(query)    

@pytest.fixture()
def db_setup_empty():
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}')

    # Define your SQL query
    query = text("""
        DROP TABLE IF EXISTS message_ids_test;
        CREATE TABLE message_ids_test (
                id VARCHAR,
                message_id VARCHAR,
                updated_at TIMESTAMPTZ
        );
    """)

    # Execute the query and fetch all results
    with engine.connect() as connection:
        result = connection.execute(query)
        
    # breakpoint()
    yield result

    # Define your SQL query
    query = text("""
        DROP TABLE IF EXISTS message_ids_test;
    """)

    # Execute the query and fetch all results
    with engine.connect() as connection:
        result = connection.execute(query)  

def test_fetch_latest_message_id(db_setup):
    db_creds = DBCredentials(
        host = DB_HOST,
        port = DB_PORT,
        user = DB_USER,
        password = DB_PASSWORD,
        database = DB_DATABASE
    )
    
    res = MessageIDsTest.fetch_latest_messageid(db_creds)

    assert res == 'id_2'

def test_fetch_latest_message_id_when_empty(db_setup_empty):
    db_creds = DBCredentials(
        host = DB_HOST,
        port = DB_PORT,
        user = DB_USER,
        password = DB_PASSWORD,
        database = DB_DATABASE
    )
    
    res = MessageIDsTest.fetch_latest_messageid(db_creds)

    assert res == None

def test_add_message_id(db_setup):
    db_creds = DBCredentials(
        host = DB_HOST,
        port = DB_PORT,
        user = DB_USER,
        password = DB_PASSWORD,
        database = DB_DATABASE
    )
    
    MessageIDsTest.add_messageid('new_id', db_creds)
    res = MessageIDsTest.fetch_latest_messageid(db_creds)
    
    assert res == 'new_id'