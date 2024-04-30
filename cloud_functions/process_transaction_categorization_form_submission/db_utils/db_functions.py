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

    try:
        engine = create_engine(f'postgresql://{db_creds.user}:{db_creds.password}@{db_creds.host}:{db_creds.port}/{db_creds.database}')
        Session = sessionmaker(bind=engine)
        session = Session()

        new_transaction = model(**data_json)

        session.add(new_transaction)
        session.commit()
        session.close()
        return True

    except Exception as e:
        
        if str(e)[:33] == '(psycopg2.errors.UniqueViolation)':
            pk_field = get_pk_field(model)
            print(f"Record with id {data_json[pk_field]} already exists in {model.__table_args__['schema']}.{model.__tablename__}")
            return False
        else:
            raise # raise original Exception

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


def update_record(
    model: Type[DeclarativeMeta], 
    db_creds: DBCredentials, 
    id: str, 
    field: str, 
    new_value: str
):
    
    engine = create_engine(f'postgresql://{db_creds.user}:{db_creds.password}@{db_creds.host}:{db_creds.port}/{db_creds.database}')
    Session = sessionmaker(bind=engine)
    session = Session()

    # Query the record you want to update
    record = session.query(model).filter_by(id=id).first()

    # Update the record
    setattr(record, field, new_value)

    # Commit the changes
    session.commit()
    session.close()
