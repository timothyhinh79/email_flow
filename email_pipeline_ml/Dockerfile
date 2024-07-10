# Use your first image as the base
FROM us-west2-docker.pkg.dev/email-parser-414818/email-flow/email-pipeline:latest

# Set the working directory
WORKDIR /app

# Copy requirements.txt and install specified packages
COPY requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt

# Copy entrypoint scripts into app/
COPY entrypoints/ /app
