FROM bitnamilegacy/spark:3.5.0

USER root

# Install Python dependencies for the batch job
RUN pip install --no-cache-dir \
    requests==2.32.3 \
    psycopg2-binary==2.9.10

# Download PostgreSQL JDBC driver (use a directory not overridden by Bitnami)
RUN mkdir -p /opt/bitnami/spark/user-jars && \
    wget -q -O /opt/bitnami/spark/user-jars/postgresql-42.7.4.jar \
    "https://jdbc.postgresql.org/download/postgresql-42.7.4.jar"

USER 1001
