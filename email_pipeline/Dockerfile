# Use an official Python runtime as a parent image
FROM python:3.10-slim-buster

# Allow statements and log messages to immediately appear in the Cloud Run logs
ENV PYTHONUNBUFFERED True

# Set the working directory in the container to /app
WORKDIR /app

# Copy requirements.txt and install specified packages
COPY requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt

# Add the current directory contents into the container at /app
COPY src/ /app/src
COPY entrypoints/ /app/entrypoints

# Make port 80 available to the world outside this container
EXPOSE 80
