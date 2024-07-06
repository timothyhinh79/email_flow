from google.cloud import secretmanager
from google.oauth2 import service_account
from google.cloud import pubsub_v1
import json

def access_secret_version(project_id, secret_id, version_id, service_account_file = None):

    # Create Secret Manager client.
    if service_account_file:
        credentials = service_account.Credentials.from_service_account_file(service_account_file)
        client = secretmanager.SecretManagerServiceClient(credentials = credentials)
    else:
        client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Return the decoded payload.
    return json.loads(response.payload.data.decode('UTF-8'))

def publish_message(project_id, topic_id, message, service_account_file=None):

    if service_account_file:
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file
        )
        publisher = pubsub_v1.PublisherClient(credentials=credentials)
    else:
        publisher = pubsub_v1.PublisherClient()

    topic_path = publisher.topic_path(project_id, topic_id)
    
    message_json = json.dumps(message)
    message_bytes = message_json.encode('utf-8')
    
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()  # Wait for the publish call to complete
        print(f"Message published to {topic_path}")
    except Exception as e:
        print(f"An error occurred: {e}")
