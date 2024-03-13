from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import json
import base64
import functions_framework

from models.history_id import HistoryIDs
from classes.db_credentials import DBCredentials
from lib.parse_helpers import access_secret_version, process_history

@functions_framework.cloud_event
def parse_data_and_save_to_db(cloud_event):

    pubsub_msg = base64.b64decode(cloud_event.data["message"]["data"])
    print("Pubsub Message")
    print(pubsub_msg)

    pubsub_msg_str = pubsub_msg.decode('utf-8')
    pubsub_msg_json = json.loads(pubsub_msg_str)
    new_history_id = pubsub_msg_json['historyId']

    api_secrets = access_secret_version('email-parser-414818', 'gmail_api_credentials', '3')
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

    try:

        gmail = build('gmail', 'v1', credentials=creds)

        # Specify the start historyId
        start_history_id = HistoryIDs.fetch_latest_historyid(db_creds)

        if not start_history_id:
            start_history_id = new_history_id

        print(f'Previous history ID: {start_history_id}')

        # Use the historyId to fetch the email history
        history = gmail.users().history().list(userId='me', startHistoryId=start_history_id).execute()
        print('History successfully retrieved')
        print(history)

        # Iterate over the history and fetch each email
        process_history(
            gmail_client = gmail,
            history=history,
            label_id="Label_3935809748622434433",
            save_to_db_=True,
            db_creds=db_creds,
        )

        HistoryIDs.add_historyid(new_history_id, db_creds)

    except Exception as error:
        # TODO(developer) - Handle errors from gmail API.
        print(f"An error occurred: {error}")