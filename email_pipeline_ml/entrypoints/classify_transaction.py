import os
import json
import logging

from confluent_kafka import Consumer, Producer, KafkaException
from google.cloud import storage
import tensorflow as tf

from src.services.gcp.gcp import access_secret_version

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Starting transactions_consumer.py")

# Get environment variables
TRANSACTIONS_TOPIC_CREDS_SECRET = os.getenv('TRANSACTIONS_TOPIC_CREDS_SECRET')
TRANSACTIONS_TOPIC_CREDS_SECRET_VER = os.getenv('TRANSACTIONS_TOPIC_CREDS_SECRET_VER')
TRANSACTIONS_TOPIC = os.getenv('TRANSACTIONS_TOPIC')
CLASSIFIED_TRANSACTIONS_TOPIC_CREDS_SECRET = os.getenv('CLASSIFIED_TRANSACTIONS_TOPIC_CREDS_SECRET')
CLASSIFIED_TRANSACTIONS_TOPIC_CREDS_SECRET_VER = os.getenv('CLASSIFIED_TRANSACTIONS_TOPIC_CREDS_SECRET_VER')
CLASSIFIED_TRANSACTIONS_TOPIC = os.getenv('CLASSIFIED_TRANSACTIONS_TOPIC')
TRANSACTIONS_TOPIC_CONSUMER_GROUP_ID = os.getenv('TRANSACTIONS_TOPIC_CONSUMER_GROUP_ID')
TOPIC_AUTO_OFFSET_RESET = os.getenv('TOPIC_AUTO_OFFSET_RESET')
TOPIC_SECURITY_PROTOCOL = os.getenv('TOPIC_SECURITY_PROTOCOL')
TOPIC_SASL_MECHANISMS = os.getenv('TOPIC_SASL_MECHANISMS')
SECRETS_ACCESS_SERVICE_ACCOUNT_FILE = os.getenv('SECRETS_ACCESS_SERVICE_ACCOUNT_FILE')
GCS_ACCESS_SERVICE_ACCOUNT_FILE = os.getenv('GCS_ACCESS_SERVICE_ACCOUNT_FILE')
CLASSIFICATION_MODEL_BUCKET = os.getenv('CLASSIFICATION_MODEL_BUCKET')
MODEL_FILE = os.getenv('MODEL_FILE')

logger.info("Environment variables loaded")

# Get credentials to read from transactions topic from Secret Manager
transactions_topic_read_creds = access_secret_version(
    'email-parser-414818', 
    TRANSACTIONS_TOPIC_CREDS_SECRET, 
    TRANSACTIONS_TOPIC_CREDS_SECRET_VER,
    SECRETS_ACCESS_SERVICE_ACCOUNT_FILE
)

# Get credentials to write to classified-transactions topic from Secret Manager
classified_transactions_topic_write_creds = access_secret_version(
    'email-parser-414818', 
    CLASSIFIED_TRANSACTIONS_TOPIC_CREDS_SECRET, 
    CLASSIFIED_TRANSACTIONS_TOPIC_CREDS_SECRET_VER,
    SECRETS_ACCESS_SERVICE_ACCOUNT_FILE
)

logger.info("Credentials loaded")

# Define the Kafka Consumer configuration
kafka_consumer_config = {
    'bootstrap.servers': transactions_topic_read_creds['bootstrap.servers'],
    'group.id': TRANSACTIONS_TOPIC_CONSUMER_GROUP_ID,
    'auto.offset.reset': TOPIC_AUTO_OFFSET_RESET,
    'security.protocol': TOPIC_SECURITY_PROTOCOL,
    'sasl.mechanisms': TOPIC_SASL_MECHANISMS,
    'sasl.username': transactions_topic_read_creds['sasl.username'],
    'sasl.password': transactions_topic_read_creds['sasl.password']
}

# Define the Kafka Producer configuration
kafka_producer_config = {
    'bootstrap.servers': classified_transactions_topic_write_creds['bootstrap.servers'],
    'security.protocol': TOPIC_SECURITY_PROTOCOL,
    'sasl.mechanisms': TOPIC_SASL_MECHANISMS,
    'sasl.username': classified_transactions_topic_write_creds['sasl.username'],
    'sasl.password': classified_transactions_topic_write_creds['sasl.password']
}
    

if __name__ == "__main__":

    # Subscribe to the Kafka topic
    consumer = Consumer(kafka_consumer_config)
    consumer.subscribe([TRANSACTIONS_TOPIC])

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
                if 'category' not in data_json: # If category is already present (e.g. from a manually logged transaction), don't overwrite it
                    data_json['category'] = predicted_category
                logger.info(f"Classified transaction ID '{data_json['id']}' as '{predicted_category}'")

                # Produce the classified transaction to the classified-transactions topic
                producer = Producer(kafka_producer_config)
                producer.produce(CLASSIFIED_TRANSACTIONS_TOPIC, json.dumps(data_json))
                producer.flush()
                logger.info(f"Produced message to topic '{CLASSIFIED_TRANSACTIONS_TOPIC}': {json.dumps(data_json)}")

    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
