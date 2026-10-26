FROM apache/spark:3.5.1-python3

# pyspark no viene pip-installed en la imagen oficial: son dos zips.
ENV PYTHONPATH=/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

# La imagen corre como usuario spark, que no puede escribir en su propio $HOME
# (/home/spark), así que pip necesita root para instalar.
USER root
RUN pip3 install --no-cache-dir --upgrade pip \
    && pip3 install --no-cache-dir \
<<<<<<< Updated upstream
        "fastapi==0.124.4" \
        "uvicorn==0.33.0" \
        "python-socketio==5.17.0" \
        "jinja2==3.1.6" \
        "kafka-python==2.2.15" \
        "redis==5.0.8"

USER spark
WORKDIR /app
COPY app.py producer.py consumer.py server_producer.py kafka_ingest.py state.py models.py redis_store.py ./
COPY templates/index.html ./templates/
=======
        "fastapi==0.115.4" \
        "uvicorn==0.32.0" \
        "python-socketio==5.11.4" \
        "jinja2==3.1.6" \
        "kafka-python==2.2.15"

USER spark
WORKDIR /app
COPY app.py producer.py consumer.py server_producer.py kafka_ingest.py state.py models.py mongo_store.py sqlite_store.py ./
COPY templates/ ./templates/
>>>>>>> Stashed changes
COPY static/ ./static/
RUN mkdir -p /app/data
EXPOSE 3003 4040

# La imagen solo trae el ejecutable python3.
CMD ["python3", "app.py"]