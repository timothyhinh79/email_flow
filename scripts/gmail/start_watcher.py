from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from common import authenticate
from common import params

def main():
  """Configures PubSub topic to watch Gmail activity
  """

  creds = authenticate.get_creds(params.token_json, params.credentials_json)

  try:
    # Call the Gmail API
    gmail = build("gmail", "v1", credentials=creds)
    request = {
      # label ID for BofA is: Label_3935809748622434433
      'labelIds': ['Label_3884943773140766149', 'Label_3935809748622434433' ], # label ID for Minim
      'topicName': 'projects/email-parser-414818/topics/personal-emails',
      'labelFilterBehavior': 'INCLUDE',
      'historyTypes': ['messageAdded']
    }
    resp = gmail.users().watch(userId='me', body=request).execute()
    print("Successfully started watch request.")

  except HttpError as error:
    # TODO(developer) - Handle errors from gmail API.
    print(f"An error occurred: {error}")


if __name__ == "__main__":
  main()