import os
import logging
import numpy as np

from confluent_kafka import Consumer
import tensorflow as tf
from tensorflow.keras import layers
from tensorflow.keras.layers import TextVectorization
from google.oauth2.credentials import Credentials

from src.services.gcp.gcp import access_secret_version
from src.services.gcs.gcs import upload_blob
from src.database.operations.db_functions import query
from src.entities.db_credentials import DBCredentials

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Starting transactions_consumer.py")

# Get environment variables
DB_CREDS_SECRET = os.getenv('DB_CREDS_SECRET')
DB_CREDS_SECRET_VER = os.getenv('DB_CREDS_SECRET_VER')
DB_TRANSACTIONS_TABLE = os.getenv('DB_TRANSACTIONS_TABLE')
DB_TRANSACTIONS_DESC_COL = os.getenv('DB_TRANSACTIONS_DESC_COL')
DB_TRANSACTIONS_CATEGORY_COL = os.getenv('DB_TRANSACTIONS_CATEGORY_COL')
GCS_CREDS_SECRET = os.getenv('GCS_CREDS_SECRET')
GCS_CREDS_SECRET_VER = os.getenv('GCS_CREDS_SECRET_VER')
GCS_ML_MODELS_BUCKET = os.getenv('GCS_ML_MODELS_BUCKET')
GCS_ML_MODEL_FILE = os.getenv('GCS_ML_MODEL_FILE')
CLASSIFIED_TRANSACTIONS_TOPIC_CREDS_SECRET = os.getenv('CLASSIFIED_TRANSACTIONS_TOPIC_CREDS_SECRET')
CLASSIFIED_TRANSACTIONS_TOPIC_CREDS_SECRET_VER = os.getenv('CLASSIFIED_TRANSACTIONS_TOPIC_CREDS_SECRET_VER')
CLASSIFIED_TRANSACTIONS_TOPIC = os.getenv('CLASSIFIED_TRANSACTIONS_TOPIC')
TRANSACTIONS_TOPIC_CONSUMER_GROUP_ID = os.getenv('TRANSACTIONS_TOPIC_CONSUMER_GROUP_ID')
TOPIC_AUTO_OFFSET_RESET = os.getenv('TOPIC_AUTO_OFFSET_RESET')
TOPIC_SECURITY_PROTOCOL = os.getenv('TOPIC_SECURITY_PROTOCOL')
TOPIC_SASL_MECHANISMS = os.getenv('TOPIC_SASL_MECHANISMS')

logger.info("Environment variables loaded")

# Get credentials to write to classified-transactions topic from Secret Manager
classified_transactions_topic_read_creds = access_secret_version(
    'email-parser-414818', 
    CLASSIFIED_TRANSACTIONS_TOPIC_CREDS_SECRET, 
    CLASSIFIED_TRANSACTIONS_TOPIC_CREDS_SECRET_VER
)

# Get credentials to write to classified-transactions topic from Secret Manager
gcs_api_creds = access_secret_version(
    'email-parser-414818', 
    GCS_CREDS_SECRET, 
    GCS_CREDS_SECRET_VER
)

# Access the DB credentials from Secret Manager
db_creds_json = access_secret_version(
    'email-parser-414818', 
    DB_CREDS_SECRET, 
    DB_CREDS_SECRET_VER,
)

logger.info("Credentials loaded")

# Define the Kafka Consumer configuration
kafka_consumer_config = {
    'bootstrap.servers': classified_transactions_topic_read_creds['bootstrap.servers'],
    'group.id': TRANSACTIONS_TOPIC_CONSUMER_GROUP_ID,
    'auto.offset.reset': TOPIC_AUTO_OFFSET_RESET,
    'security.protocol': TOPIC_SECURITY_PROTOCOL,
    'sasl.mechanisms': TOPIC_SASL_MECHANISMS,
    'sasl.username': classified_transactions_topic_read_creds['sasl.username'],
    'sasl.password': classified_transactions_topic_read_creds['sasl.password']
}

if __name__ == "__main__":

    # Subscribe to the Kafka topic
    consumer = Consumer(kafka_consumer_config)
    consumer.subscribe([CLASSIFIED_TRANSACTIONS_TOPIC])

    categories_num_map = {
        'Food': 0, 'Personal & Miscellaneous': 1, 'Savings & Investments': 2,
        'Entertainment': 3, 'Education': 4, 'Living Expenses': 5,
        'Transportation': 6, 'Healthcare': 7, 'Travel': 8,
    }
    categories_text = list(categories_num_map.keys())


    ### Train RNN model on entire dataset, then save
    # Create DBCredentials object
    db_creds = DBCredentials(
        host = db_creds_json['DB_HOST'],
        port = db_creds_json['DB_PORT'],
        user = db_creds_json['DB_USER'],
        password = db_creds_json['DB_PASSWORD'],
        database = db_creds_json['DB_DATABASE'],
    )
    data = query(f"SELECT {DB_TRANSACTIONS_DESC_COL}, {DB_TRANSACTIONS_CATEGORY_COL} FROM {DB_TRANSACTIONS_TABLE}", db_creds)
    descriptions = np.array([row[0] for row in data]).reshape(-1)
    categories = np.array([categories_num_map[row[1]] for row in data])

    # Create a TextVectorization layer
    vectorizer = TextVectorization(output_mode='int')
    vectorizer.adapt(descriptions)

    # Create the model
    model = tf.keras.models.Sequential([
        vectorizer,
        layers.Embedding(input_dim=len(vectorizer.get_vocabulary()), output_dim=64, mask_zero=True),
        layers.Bidirectional(layers.LSTM(64)),
        layers.Dense(32, activation='relu'),
        layers.Dense(len(np.unique(categories_text)), activation='softmax')
    ])

    # Compile and fit the model
    model.compile(loss='sparse_categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
    model.fit(descriptions, categories, epochs=10)

    # Save the model

    # make models/ directory if it doesn't exist
    if not os.path.exists('models'):
        os.makedirs('models')
    local_model_file = f'models/{GCS_ML_MODEL_FILE}'
    model.save(local_model_file)

    # Get credentials to access the GCS bucket from Secret Manager
    gcs_creds = Credentials.from_authorized_user_info({
        'client_id': gcs_api_creds['client_id'], 
        'client_secret': gcs_api_creds['client_secret'],
        'refresh_token': gcs_api_creds['refresh_token']
    })

    # Upload the model to GCS
    upload_blob(gcs_creds, 'email-parser-414818', 
                GCS_ML_MODELS_BUCKET, 
                local_model_file, 
                GCS_ML_MODEL_FILE)