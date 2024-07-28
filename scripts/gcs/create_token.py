import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Define access scopes to be granted for Google Cloud Storage API
SCOPES = ["https://www.googleapis.com/auth/devstorage.read_write"]

# Define client_secrets_json containing Client ID + Client Secret
client_secrets_json = "credentials/client_secrets.json"

# Define where token.json should be generated, containing the Refresh Token
token_json = "credentials/token.json"

# Invoke OAuth2 Flow to securely grant permission for Google Cloud Storage API scoped access
flow = InstalledAppFlow.from_client_secrets_file(
    client_secrets_json, SCOPES
)
creds = flow.run_local_server(port=0)

with open(token_json, "w") as token:
    token.write(creds.to_json())