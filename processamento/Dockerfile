FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=python3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Driver JDBC usado pelo Spark pra gravar os agregados no Postgres
RUN curl -L -o /opt/postgresql-jdbc.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

COPY spark_job.py .

CMD ["python", "spark_job.py"]
