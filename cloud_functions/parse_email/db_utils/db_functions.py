from sqlalchemy import create_engine, text
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


def save_to_db(model: Type[DeclarativeMeta], data_json: dict, db_creds: DBCredentials):

    # Create a SQLAlchemy engine
    engine = create_engine(f'postgresql://{db_creds.user}:{db_creds.password}@{db_creds.host}:{db_creds.port}/{db_creds.database}')

    Session = sessionmaker(bind=engine)
    session = Session()

    new_transaction = model(**data_json)

    session.add(new_transaction)
    session.commit()
    session.close()