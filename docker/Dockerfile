FROM apache/airflow:2.9.3

USER root

# System dependencies (optional but useful)
RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean

USER airflow

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt