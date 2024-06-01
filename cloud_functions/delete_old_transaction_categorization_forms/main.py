from google.oauth2.credentials import Credentials
import base64
import os
from datetime import datetime, timedelta
import functions_framework

from lib.gcp.gcp import access_secret_version
from lib.google_drive.google_drive import (
    delete_file, 
    query_files,
)

# Accessing environment variables
days_past_threshold = os.getenv('DAYS_PAST_THRESHOLD')
google_drive_api_creds_secret = os.getenv('GOOGLE_DRIVE_API_CREDS_SECRET')
google_drive_api_creds_secret_ver = os.getenv('GOOGLE_DRIVE_API_CREDS_SECRET_VER')

@functions_framework.cloud_event
def delete_old_transaction_categorization_forms(cloud_event):

    pubsub_msg = base64.b64decode(cloud_event.data["message"]["data"])
    print("Pubsub Message")
    print(pubsub_msg)

    ##### Load all necessary credentials

    # Setting up Google Drive API credentials
    google_drive_api_secrets = access_secret_version('email-parser-414818', google_drive_api_creds_secret, google_drive_api_creds_secret_ver)
    google_drive_creds = Credentials.from_authorized_user_info({
        'client_id': google_drive_api_secrets['client_id'], 
        'client_secret': google_drive_api_secrets['client_secret'],
        'refresh_token': google_drive_api_secrets['refresh_token']
    })

    ##### Delete transaction categorization forms older than days_past_threshold

    # get datetime from days_past_threshold days ago
    cutoff_date = datetime.now() - timedelta(days=int(days_past_threshold))
    cutoff_date_str = cutoff_date.strftime('%Y-%m-%dT%H:%M:%S')

    # query for files to delete
    print(f"Deleting transaction categorization forms created before {cutoff_date_str}")
    query = f"mimeType = 'application/vnd.google-apps.form' and createdTime < '{cutoff_date_str}'"
    query += f" and name contains 'Categorize \"' and name contains '\" Transaction'"
    query += f" and not name contains 'FOR PYTEST'"
    files_to_delete = query_files(google_creds=google_drive_creds, query=query)

    # delete files
    for file in files_to_delete:
        delete_file(google_creds=google_drive_creds, file_id=file['id'])

    print('Done')
