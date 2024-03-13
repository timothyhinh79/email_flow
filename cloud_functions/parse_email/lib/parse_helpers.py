
from google.cloud import secretmanager
import json
import re
import uuid
import datetime

def access_secret_version(project_id, secret_id, version_id):
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Return the decoded payload.
    return json.loads(response.payload.data.decode('UTF-8'))

def parse_email_body(email_body):
    amount_pattern = r"Amount:\s*\*\$(\d+\.\d{2})\*"
    date_pattern = r"Date:\s*\*(\w+\s\d{2},\s\d{4})\*"
    where_pattern = r"Where:\s*\*(.*)\*"
    
    amount_search = re.search(amount_pattern, email_body)
    date_search = re.search(date_pattern, email_body)
    where_search = re.search(where_pattern, email_body)

    assert amount_search and date_search and where_search, "Amount + Date + Where not found in credit card transaction email from BofA"

    amount = amount_search.group(1)
    date = date_search.group(1)
    where = where_search.group(1)

    transaction_type = 'credit'

    data_json = {
        'id': uuid.uuid5(uuid.NAMESPACE_DNS, '-'.join([amount, date, where])),
        'transaction_type': transaction_type,
        'amount': amount,
        'transaction_date': date,
        'description': where,
        'category': None,
        'updated_at': datetime.datetime.now(datetime.timezone.utc)
    }

    return data_json