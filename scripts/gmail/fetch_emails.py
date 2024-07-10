import argparse
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import base64

from common import authenticate, params

def main(args):
  """ Fetches user's emails starting from a history ID
  """
  
  creds = authenticate.get_creds(params.token_json, params.credentials_json)

  try:

    gmail = build('gmail', 'v1', credentials=creds)

    # Specify the start historyId
    start_history_id = args.history_id

    # Use the historyId to fetch the email history
    history = gmail.users().history().list(userId='me', startHistoryId=start_history_id).execute()

    # Iterate over the history and fetch each email
    for history_record in history['history']:
        if 'messagesAdded' in history_record:
            for messageAdded in history_record['messagesAdded']:
                
                message = messageAdded['message']
                if 'labelIds' in message and 'Label_3884943773140766149' in message['labelIds']:
                  msg = gmail.users().messages().get(userId='me', id=message['id']).execute()
                  # print(msg)

                  if 'data' in msg['payload']['body']:
                      data = msg['payload']['body']['data']
                  else:
                      # if email is multipart, get the first part, which is typically the plain text version of the email body
                      parts = msg['payload'].get('parts')[0]
                      data = parts['body']['data']

                  # Decode the body data
                  data = data.replace("-","+").replace("_","/")
                  decoded_data = base64.urlsafe_b64decode(data)
                  body = decoded_data.decode("utf-8")
                  print(body)
                  print('========================================================')

  except HttpError as error:
    # TODO(developer) - Handle errors from gmail API.
    print(f"An error occurred: {error}")


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('--history-id', type=str)

  args = parser.parse_args()
  main(args)
