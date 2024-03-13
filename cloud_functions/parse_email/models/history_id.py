from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from db_utils.db_functions import query, save_to_db
import uuid
import datetime

Base = declarative_base()

class HistoryIDs(Base):
    __tablename__ = 'history_ids'
    __table_args__ = {'schema': 'public'}

    id = Column(String, primary_key=True)
    history_id = Column(String)
    updated_at = Column(DateTime)

    @classmethod
    def fetch_latest_historyid(cls, db_creds):
        res = query(
            sql = f'SELECT history_id FROM {cls.__table_args__["schema"]}.{cls.__tablename__} ORDER BY updated_at DESC LIMIT 1',
            db_creds = db_creds
        )
        
        if not res:
            return None
        else:
            return res[0][0]
        
    @classmethod
    def add_historyid(cls, history_id, db_creds):
        current_time = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')
        data_json = {
            'id': uuid.uuid5(uuid.NAMESPACE_DNS, '-'.join([str(history_id), current_time])),
            'history_id': history_id,
            'updated_at': current_time
        }
        
        save_to_db(cls, data_json, db_creds)


class HistoryIDsTest(Base):
    __tablename__ = 'history_ids_test'
    __table_args__ = {'schema': 'public'}

    id = Column(String, primary_key=True)
    history_id = Column(String)
    updated_at = Column(DateTime)

    @classmethod
    def fetch_latest_historyid(cls, db_creds):
        res = query(
            sql = f'SELECT history_id FROM {cls.__table_args__["schema"]}.{cls.__tablename__} ORDER BY updated_at DESC LIMIT 1',
            db_creds = db_creds
        )
        
        if not res:
            return None
        else:
            return res[0][0]
        
    @classmethod
    def add_historyid(cls, history_id, db_creds):
        current_time = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')
        data_json = {
            'id': uuid.uuid5(uuid.NAMESPACE_DNS, '-'.join([str(history_id), current_time])),
            'history_id': history_id,
            'updated_at': current_time
        }

        save_to_db(cls, data_json, db_creds)