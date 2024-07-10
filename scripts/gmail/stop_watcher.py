from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from common import authenticate
from common import params

def main():
  """Configures PubSub topic to stop watching Gmail activity
  """

  creds = authenticate.get_creds(params.token_json, params.credentials_json)

  try:
    # Call the Gmail API
    gmail = build("gmail", "v1", credentials=creds)
    resp = gmail.users().stop(userId='me').execute()
    print("Successfully stopped watch request.")
    print(resp)
    # breakpoint()

  except HttpError as error:
    # TODO(developer) - Handle errors from gmail API.
    print(f"An error occurred: {error}")


if __name__ == "__main__":
  main()