import os
import json
import logging

from confluent_kafka import Consumer, KafkaException
from google.cloud import storage
import tensorflow as tf

from lib.gcp.gcp import access_secret_version
from db_utils.db_functions import save_to_db
from classes.db_credentials import DBCredentials
from models.financial_transaction import FinancialTransaction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Starting transactions_consumer.py")

# Get environment variables
TRANSACTIONS_TOPIC_CREDS_SECRET = os.getenv('TRANSACTIONS_TOPIC_CREDS_SECRET')
TRANSACTIONS_TOPIC_CREDS_SECRET_VER = os.getenv('TRANSACTIONS_TOPIC_CREDS_SECRET_VER')
TRANSACTIONS_TOPIC = os.getenv('TRANSACTIONS_TOPIC')
TRANSACTIONS_TOPIC_CONSUMER_GROUP_ID = os.getenv('TRANSACTIONS_TOPIC_CONSUMER_GROUP_ID')
TOPIC_AUTO_OFFSET_RESET = os.getenv('TOPIC_AUTO_OFFSET_RESET')
TOPIC_SECURITY_PROTOCOL = os.getenv('TOPIC_SECURITY_PROTOCOL')
TOPIC_SASL_MECHANISMS = os.getenv('TOPIC_SASL_MECHANISMS')
DB_CREDS_SECRET = os.getenv('DB_CREDS_SECRET')
DB_CREDS_SECRET_VER = os.getenv('DB_CREDS_SECRET_VER')
SECRETS_ACCESS_SERVICE_ACCOUNT_FILE = os.getenv('SECRETS_ACCESS_SERVICE_ACCOUNT_FILE')
GCS_ACCESS_SERVICE_ACCOUNT_FILE = os.getenv('GCS_ACCESS_SERVICE_ACCOUNT_FILE')
CLASSIFICATION_MODEL_BUCKET = os.getenv('CLASSIFICATION_MODEL_BUCKET')
MODEL_FILE = os.getenv('MODEL_FILE')

logger.info("Environment variables loaded")

# Access the Kafka credentials from Secret Manager
kafka_credentials = access_secret_version(
    'email-parser-414818', 
    TRANSACTIONS_TOPIC_CREDS_SECRET, 
    TRANSACTIONS_TOPIC_CREDS_SECRET_VER,
    SECRETS_ACCESS_SERVICE_ACCOUNT_FILE
)

# Access the DB credentials from Secret Manager
db_creds_json = access_secret_version(
    'email-parser-414818', 
    DB_CREDS_SECRET, 
    DB_CREDS_SECRET_VER,
    SECRETS_ACCESS_SERVICE_ACCOUNT_FILE
)

logger.info("Credentials loaded")

# Create DBCredentials object
db_creds = DBCredentials(
    host = db_creds_json['DB_HOST'],
    port = db_creds_json['DB_PORT'],
    user = db_creds_json['DB_USER'],
    password = db_creds_json['DB_PASSWORD'],
    database = db_creds_json['DB_DATABASE'],
)

# Define the Kafka Consumer configuration
kafka_consumer_config = {
    'bootstrap.servers': kafka_credentials['bootstrap.servers'],
    'group.id': TRANSACTIONS_TOPIC_CONSUMER_GROUP_ID,
    'auto.offset.reset': TOPIC_AUTO_OFFSET_RESET,
    'security.protocol': TOPIC_SECURITY_PROTOCOL,
    'sasl.mechanisms': TOPIC_SASL_MECHANISMS,
    'sasl.username': kafka_credentials['sasl.username'],
    'sasl.password': kafka_credentials['sasl.password']
}

def create_consumer(config = None):
    consumer = Consumer(config)
    return consumer

def consume_messages(consumer, topic):
    # Subscribe to the Kafka topic
    consumer.subscribe([topic])

    try:
        while True:
            # Poll for messages
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Load the JSON message from topic
                data_json = json.loads(msg.value().decode('utf-8'))
                logger.info(f"Received message from topic: {msg.value().decode('utf-8')}")

                categories_text = [
                    'Food', 'Personal & Miscellaneous', 'Savings & Investments',
                    'Entertainment', 'Education', 'Living Expenses'
                ]

                # Load classification model
                gcs = storage.Client.from_service_account_json(GCS_ACCESS_SERVICE_ACCOUNT_FILE)
                bucket = gcs.get_bucket(CLASSIFICATION_MODEL_BUCKET)
                blob = bucket.blob(MODEL_FILE)
                blob.download_to_filename(MODEL_FILE) 
                model = tf.keras.models.load_model(MODEL_FILE)
                logger.info("Loaded classification model")

                # Predict category based on description
                predicted_category = categories_text[model.predict([data_json['description']])[0].argmax(axis=-1)]
                data_json['category_ml'] = predicted_category
                data_json['category'] = predicted_category
                logger.info(f"Classified transaction ID '{data_json['id']}' as '{predicted_category}'")

                # Save classifed transaction to DB
                res = save_to_db(FinancialTransaction, data_json, db_creds)
                if res:
                    logger.info(f"Saved message to DB: {msg.value().decode('utf-8')}")
                else:
                    logger.error(f"Following message already exists in DB: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == "__main__":
    consumer = create_consumer(kafka_consumer_config)
    consume_messages(consumer, TRANSACTIONS_TOPIC)
