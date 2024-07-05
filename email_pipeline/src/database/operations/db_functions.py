from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker, DeclarativeMeta

from typing import Type

from src.entities.db_credentials import DBCredentials

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

def insert_record(model: Type[DeclarativeMeta], data_json: dict, db_creds: DBCredentials):

    new_transaction = model(**data_json)

    try:
        engine = create_engine(f'postgresql://{db_creds.user}:{db_creds.password}@{db_creds.host}:{db_creds.port}/{db_creds.database}')
        Session = sessionmaker(bind=engine)
        session = Session()

        session.add(new_transaction)
        session.commit()
        return {'status': 'inserted'}

    # If attempting to insert a record with a primary key that already exists, update the record instead
    except IntegrityError as e:
        session.rollback()
        if 'unique constraint' in str(e.orig).lower():
            pk_field = get_pk_field(model)
            print(f"Record with id {data_json[pk_field]} already exists in {model.__table_args__['schema']}.{model.__tablename__}")
            return {'status': 'failed to insert duplicate record'}
    
    # Handle other unexpected errors
    except Exception as e:
        session.rollback()
        print(f"An unexpected error occurred: {e}")
        raise

    finally:
        session.close()

def update_record(model: Type[DeclarativeMeta], data_json: dict, db_creds: DBCredentials):

    new_transaction = model(**data_json)

    try:

        engine = create_engine(f'postgresql://{db_creds.user}:{db_creds.password}@{db_creds.host}:{db_creds.port}/{db_creds.database}')
        Session = sessionmaker(bind=engine)
        session = Session()

        pk_field = get_pk_field(model)
        print(f"Record with id {data_json[pk_field]} already exists in {model.__table_args__['schema']}.{model.__tablename__}")
        print(f"Will update record with id {data_json[pk_field]} instead.")
        session.merge(new_transaction)
        session.commit()
        return {'status': 'updated'}
    
    # Handle other unexpected errors
    except Exception as e:
        session.rollback()
        print(f"An unexpected error occurred: {e}")
        raise

    finally:
        session.close()

def upsert_record(model: Type[DeclarativeMeta], data_json: dict, db_creds: DBCredentials):

    insert_res = insert_record(model, data_json, db_creds)
    if insert_res['status'] == 'inserted':
        return insert_res
    elif insert_res['status'] == 'failed to insert duplicate record':
        return update_record(model, data_json, db_creds)

def find_record(
    model: Type[DeclarativeMeta], 
    db_creds: DBCredentials, 
    id: str,
):
    engine = create_engine(f'postgresql://{db_creds.user}:{db_creds.password}@{db_creds.host}:{db_creds.port}/{db_creds.database}')
    Session = sessionmaker(bind=engine)
    session = Session()

    # Query the record you want to update
    record = session.query(model).filter_by(id=id).first()

    session.close()

    return record
