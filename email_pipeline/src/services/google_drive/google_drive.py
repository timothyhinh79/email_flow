from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.credentials import Credentials

def delete_file(google_creds, file_id):

    # Build the service
    drive_service = build('drive', 'v3', credentials=google_creds)

    # Delete the file
    try:
        res = drive_service.files().delete(fileId=file_id).execute()
        print(f'File with id {file_id} has been deleted.')
        return res
    except Exception as e:
        if "'message': 'File not found:" in str(e):
            print(f'File with id {file_id} was not found. Skipping deletion...')
        else:
            raise

def query_files(google_creds, query):

    # Build the service
    drive_service = build('drive', 'v3', credentials=google_creds)

    # Query the files
    results = drive_service.files().list(q=query, fields="nextPageToken, files(id, name, mimeType, createdTime)").execute()
    files = results.get('files', [])

    return files
