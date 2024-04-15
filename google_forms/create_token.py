from apiclient import discovery
from httplib2 import Http
from oauth2client import client, file, tools
from google.oauth2.credentials import Credentials
import os
from dotenv import load_dotenv

load_dotenv()

GOOGLE_CLIENT_ID = os.getenv('GOOGLE_CLIENT_ID')
GOOGLE_CLIENT_SECRET = os.getenv('GOOGLE_CLIENT_SECRET')
GOOGLE_REFRESH_TOKEN = os.getenv('GOOGLE_REFRESH_TOKEN')

SCOPES = ["https://www.googleapis.com/auth/forms.body","https://www.googleapis.com/auth/forms.responses.readonly"]
DISCOVERY_DOC = "https://forms.googleapis.com/$discovery/rest?version=v1"

token_json = "credentials/token.json"

store = file.Storage(token_json)
creds = None

flow = client.flow_from_clientsecrets("credentials/client_secrets.json", SCOPES)
creds = tools.run_flow(flow, store)
