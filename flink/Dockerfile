FROM flink:1.20.1-scala_2.12-java11

# Install Python3, pip, and development headers
USER root
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev \
        wget curl && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python

# Install PyFlink matching the Flink version and runtime dependencies
RUN pip3 install --no-cache-dir \
    apache-flink==1.20.1 \
    psycopg2-binary==2.9.10 \
    kafka-python==2.1.5

# Download the Flink Kafka connector JAR (required for KafkaSource)
RUN mkdir -p /opt/flink/usrlib && \
    wget -q -O /opt/flink/usrlib/flink-sql-connector-kafka-3.3.0-1.20.jar \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar"

USER flink
