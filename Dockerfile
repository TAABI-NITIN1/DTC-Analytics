FROM apache/airflow:3.1.7

# Install build tools for psycopg2 if needed
USER root
RUN apt-get update \
    && apt-get install -y build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY docker/requirements.txt /requirements.txt
RUN pip install --no-cache-dir --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.7/constraints-3.12.txt" -r /requirements.txt

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
