from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker, DeclarativeMeta

from typing import Type

from classes.db_credentials import DBCredentials

def query(sql: str, db_creds: DBCredentials):
    engine = create_engine(f'postgresql://{db_creds.user}:{db_creds.password}@{db_creds.host}:{db_creds.port}/{db_creds.database}')

    # Define your SQL query
    query = text(sql)

    # Execute the query and fetch all results
    with engine.connect() as connection:
        result = connection.execute(query)
        rows = result.fetchall()

    return rows

def get_pk_field(model: Type[DeclarativeMeta]):
    inspector = inspect(model)
    for key, column in inspector.columns.items():
        if column.primary_key:
            return key

def save_to_db(model: Type[DeclarativeMeta], data_json: dict, db_creds: DBCredentials):

    # Confirm if Primary Key value already exists in table. If so, do not save to database

    pk_field = get_pk_field(model)
    sql_to_find_id = f"""
        SELECT * 
        FROM {model.__table_args__['schema']}.{model.__tablename__} 
        WHERE {pk_field}::VARCHAR = '{data_json[pk_field]}'
    """
    res = query(sql_to_find_id, db_creds)
    if res:
        print(f"Record with {pk_field} = '{data_json[pk_field]}' already exists in {model.__table_args__['schema']}.{model.__tablename__} ")
        return
    
    # If Primary Key value does not exist, proceed with saving to database
    engine = create_engine(f'postgresql://{db_creds.user}:{db_creds.password}@{db_creds.host}:{db_creds.port}/{db_creds.database}')
    Session = sessionmaker(bind=engine)
    session = Session()

    new_transaction = model(**data_json)

    session.add(new_transaction)
    session.commit()
    session.close()