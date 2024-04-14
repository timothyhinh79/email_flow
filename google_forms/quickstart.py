from apiclient import discovery
from google.oauth2.credentials import Credentials
import os
from dotenv import load_dotenv

load_dotenv()

GOOGLE_CLIENT_ID = os.getenv('GOOGLE_CLIENT_ID')
GOOGLE_CLIENT_SECRET = os.getenv('GOOGLE_CLIENT_SECRET')
GOOGLE_REFRESH_TOKEN = os.getenv('GOOGLE_REFRESH_TOKEN')

SCOPES = "https://www.googleapis.com/auth/forms.body"
DISCOVERY_DOC = "https://forms.googleapis.com/$discovery/rest?version=v1"

# Create credentials
creds = Credentials.from_authorized_user_info({
    'client_id': GOOGLE_CLIENT_ID, 
    'client_secret': GOOGLE_CLIENT_SECRET,
    'refresh_token': GOOGLE_REFRESH_TOKEN
})

form_service = discovery.build(
    "forms",
    "v1",
    credentials=creds,
    discoveryServiceUrl=DISCOVERY_DOC,
    static_discovery=False,
)

# token_json = "token.json"

# store = file.Storage(token_json)
# creds = None
# if os.path.exists(token_json):
#     creds = Credentials.from_authorized_user_file(token_json, SCOPES)
#     form_service = discovery.build(
#         "forms",
#         "v1",
#         credentials=creds,
#         discoveryServiceUrl=DISCOVERY_DOC,
#         static_discovery=False,
#     )
# elif not creds or creds.invalid:
#     flow = client.flow_from_clientsecrets("credentials/client_secrets.json", SCOPES)
#     creds = tools.run_flow(flow, store)

#     form_service = discovery.build(
#         "forms",
#         "v1",
#         http=creds.authorize(Http()),
#         discoveryServiceUrl=DISCOVERY_DOC,
#         static_discovery=False,
#     )

# Request body for creating a form
NEW_FORM = {
    "info": {
        "title": "Quickstart form",
    }
}

# Request body to add a multiple-choice question
NEW_QUESTION = {
    "requests": [
        {
            "createItem": {
                "item": {
                    "title": (
                        "In what year did the United States land a mission on"
                        " the moon?"
                    ),
                    "questionItem": {
                        "question": {
                            "required": True,
                            "choiceQuestion": {
                                "type": "RADIO",
                                "options": [
                                    {"value": "1965"},
                                    {"value": "1967"},
                                    {"value": "1969"},
                                    {"value": "1971"},
                                ],
                                "shuffle": True,
                            },
                        }
                    },
                },
                "location": {"index": 0},
            }
        }
    ]
}

# Creates the initial form
result = form_service.forms().create(body=NEW_FORM).execute()

# Adds the question to the form
question_setting = (
    form_service.forms()
    .batchUpdate(formId=result["formId"], body=NEW_QUESTION)
    .execute()
)

# Prints the result to show the question has been added
get_result = form_service.forms().get(formId=result["formId"]).execute()
print(get_result)
