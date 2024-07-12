# Email Flow Pipeline

## Overview

This pipeline is designed to automatically parse emails for relevant financial transaction data and save the extracted information into a PostgreSQL database. It includes a machine learning (ML) component that classifies transactions automatically. Additionally, it provides a mechanism for manual re-classification of transactions if the ML model misclassifies them. Future enhancements include automated model retraining and a live dashboard.

## Features

- **Real-Time Email Parsing**: Automatically parses incoming emails for financial transaction data.
- **Data Storage**: Saves parsed transaction data into a PostgreSQL database.
- **ML Classification**: Classifies transactions using a machine learning model.
- **Manual Re-Classification**: Allows manual re-classification of transactions.
- **Future Enhancements**:
  - Automated model retraining as new data comes in.
  - Live dashboard with a connection to the PostgreSQL database.

## Components

1. **parse_email**
   - Monitors email inbox for new messages.
   - Extracts relevant financial transaction data.
   - Pushes to `transactions` Kafka topic.

2. **classify_transactions**
   - Uses a Recurrent Neural Network to classify incoming transactions.
   - Pushes classified transaction to `classified_transactions` topic.
   - Sends google form to user to reclassify the transaction if necessary.

3. **process_transaction_categorization_form_submission**
   - Processes google form submissions for manual re-classifications and updates the corresponding database record.

4. **log_transaction_form_submission**
   - Enables user to manually log transactions not automatically captured by the pipeline.

5. **write_to_db**
   - Sends classified transaction data to Postgres database.

6. **Data Storage**
   - PostgreSQL database to store transaction data.
   - Schema designed to efficiently store and query financial transactions.

7. **ML Classifier**
   - Recurrent Neural Network to classify transactions.
   - Model trained on historical transaction data.
   - Classification results are saved to Postgres database

## Deployment
   - **email-pipeline** and **email-pipeline-ml** Docker images contain the core codebase and various entrypoints to carry out specific tasks for different parts of the pipeline. 
   - Uploaded to Google's Artifact Registry and deployed on Cloud Run services triggered via PubSub topics.

## Next Steps

1. **Automated Model Retraining**
   - Implement a mechanism to automatically retrain the ML model as new data is added to the database.

2. **Live Dashboard**
   - Develop a dashboard with a live connection to the PostgreSQL database.
   - Visualize transaction data and classification results in real-time.
