# Use an official Python runtime as a parent image
FROM python:3.10-slim-buster

# Set the working directory in the container to /app
WORKDIR /app

# Copy requirements.txt and install specified packages
COPY requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt

# Add the current directory contents into the container at /app
COPY classes/ /app/classes
COPY db_utils/ /app/db_utils
COPY lib/ /app/lib
COPY models/ /app/models
COPY transactions_consumer.py /app

# Make port 80 available to the world outside this container
EXPOSE 80

# Run the command to start the consumer
CMD ["python", "transactions_consumer.py"]
