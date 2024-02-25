from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from common import authenticate
from common import params

def main():
  """Shows basic usage of the Gmail API.
  Lists the user's Gmail labels.
  """

  creds = authenticate.get_creds(params.token_json, params.credentials_json)

  try:
    # Call the Gmail API
    gmail = build("gmail", "v1", credentials=creds)

    # Get the list of all labels
    results = gmail.users().labels().list(userId='me').execute()
    labels = results.get('labels', [])

    # Print the ID of each label
    for label in labels:
        print(f"Label ID: {label['id']}, Label Name: {label['name']}")

  except HttpError as error:
    # TODO(developer) - Handle errors from gmail API.
    print(f"An error occurred: {error}")


if __name__ == "__main__":
  main()