from apiclient import discovery
from httplib2 import Http
from oauth2client import client, file, tools
from google.oauth2.credentials import Credentials
import os
from dotenv import load_dotenv

load_dotenv()

# Define access scopes to be granted for Google Forms API
SCOPES = ["https://www.googleapis.com/auth/forms.body","https://www.googleapis.com/auth/forms.responses.readonly"]
DISCOVERY_DOC = "https://forms.googleapis.com/$discovery/rest?version=v1"

# Define client_secrets_json containing Client ID + Client Secret
client_secrets_json = "credentials/client_secrets.json"

# Define where token.json should be generated, containing the Refresh Token
token_json = "credentials/token.json"

# Invoke OAuth2 Flow to securely grant permission for Google Forms API scoped access
store = file.Storage(token_json)
flow = client.flow_from_clientsecrets(client_secrets_json, SCOPES)
creds = tools.run_flow(flow, store)
