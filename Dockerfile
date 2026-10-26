FROM apache/spark:3.5.1-python3

# pyspark no viene pip-installed en la imagen oficial
ENV PYTHONPATH=/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

USER root
RUN pip3 install --no-cache-dir --upgrade pip \
    && pip3 install --no-cache-dir \
        "fastapi==0.128.0" \
        "uvicorn==0.44.0" \
        "python-socketio==5.17.0" \
        "jinja2==3.1.6" \
        "kafka-python==2.2.15" \
        "pymongo==4.10.1"

USER spark
WORKDIR /app
COPY app.py producer.py consumer.py server_producer.py kafka_ingest.py state.py models.py mongo_store.py ./
COPY templates/ ./templates/
COPY static/ ./static/
EXPOSE 3003 4040

CMD ["python3", "app.py"]
