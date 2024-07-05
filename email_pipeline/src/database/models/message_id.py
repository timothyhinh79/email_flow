from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from src.database.operations.db_functions import query, save_to_db
import uuid
import datetime

Base = declarative_base()

class MessageIDs(Base):
    __tablename__ = 'message_ids'
    __table_args__ = {'schema': 'public'}

    id = Column(String, primary_key=True)
    message_id = Column(String)
    updated_at = Column(DateTime)

    @classmethod
    def fetch_latest_messageid(cls, db_creds):
        res = query(
            sql = f'SELECT message_id FROM {cls.__table_args__["schema"]}.{cls.__tablename__} ORDER BY updated_at DESC LIMIT 1',
            db_creds = db_creds
        )
        
        if not res:
            return None
        else:
            return res[0][0]
        
    @classmethod
    def add_messageid(cls, message_id, db_creds):
        current_time = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')
        data_json = {
            'id': str(uuid.uuid5(uuid.NAMESPACE_DNS, '-'.join([str(message_id), current_time]))),
            'message_id': message_id,
            'updated_at': current_time
        }
        
        save_to_db(cls, data_json, db_creds)


class MessageIDsTest(Base):
    __tablename__ = 'message_ids_test'
    __table_args__ = {'schema': 'public'}

    id = Column(String, primary_key=True)
    message_id = Column(String)
    updated_at = Column(DateTime)

    @classmethod
    def fetch_latest_messageid(cls, db_creds):
        res = query(
            sql = f'SELECT message_id FROM {cls.__table_args__["schema"]}.{cls.__tablename__} ORDER BY updated_at DESC LIMIT 1',
            db_creds = db_creds
        )
        
        if not res:
            return None
        else:
            return res[0][0]
        
    @classmethod
    def add_messageid(cls, message_id, db_creds):
        current_time = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')
        data_json = {
            'id': str(uuid.uuid5(uuid.NAMESPACE_DNS, '-'.join([str(message_id), current_time]))),
            'message_id': message_id,
            'updated_at': current_time
        }

        save_to_db(cls, data_json, db_creds)