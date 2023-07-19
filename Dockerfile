FROM google/cloud-sdk:slim

# Copy jobs into working directory:
COPY ./python /app
WORKDIR /app

# Install dependencies:
RUN pip3 install --upgrade pip && pip3 install -r requirements.txt
RUN apt update && apt install -y git

# Set env variables:
ENV PYTHONPATH /app