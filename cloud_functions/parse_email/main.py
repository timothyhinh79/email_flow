from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import json
import base64
import functions_framework

from models.message_id import MessageIDs
from classes.db_credentials import DBCredentials
from lib.parse_helpers import (
    access_secret_version,
    get_messages_after_specific_message, 
    process_message,
    get_latest_message_id
)

@functions_framework.cloud_event
def parse_data_and_save_to_db(cloud_event):

    pubsub_msg = base64.b64decode(cloud_event.data["message"]["data"])
    print("Pubsub Message")
    print(pubsub_msg)

    api_secrets = access_secret_version('email-parser-414818', 'gmail_api_credentials', '6')
    db_creds_json = access_secret_version('email-parser-414818', 'gmail_logger_postgres_credentials', '1')
    db_creds = DBCredentials(
        host = db_creds_json['DB_HOST'],
        port = db_creds_json['DB_PORT'],
        user = db_creds_json['DB_USER'],
        password = db_creds_json['DB_PASSWORD'],
        database = db_creds_json['DB_DATABASE'],
    )

    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': api_secrets['client_id'], 
        'client_secret': api_secrets['client_secret'],
        'refresh_token': api_secrets['refresh_token']
    })

    gmail = build('gmail', 'v1', credentials=creds)

    # Specify the start messageId
    start_message_id = MessageIDs.fetch_latest_messageid(db_creds)

    if not start_message_id:
        start_message_id = None

    print(f'Previous message ID: {start_message_id}')

    messages = get_messages_after_specific_message(gmail, start_message_id, label_ids=['Label_3935809748622434433'])

    if messages:
        for message in messages:
            process_message(gmail, message['id'], save_to_db_= True, db_creds = db_creds)

        latest_message_id = get_latest_message_id(gmail, messages)
        MessageIDs.add_messageid(latest_message_id, db_creds)
