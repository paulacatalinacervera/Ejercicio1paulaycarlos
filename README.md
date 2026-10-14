# Telemetry & Matchmaking Pipeline (copia limpia)

Sistema de **telemetría y matchmaking en tiempo real** (Producer → Apache Spark → Dashboard),
con la telemetría de servidores de juego viajando por **Kafka** (`telemetry.server.metrics`).

Esta carpeta es autocontenida: solo el servicio Python, sin wrapper Next.js ni dependencias
de Node. El dashboard lo sirve FastAPI directamente.

## Requisitos

- Python 3.11+ (probado 3.12) y Java 17+ (para Spark).
- Opcional: Docker para correr contra el clúster Spark standalone y Kafka.

## Correr en local (Spark local, Kafka del host)

```powershell
powershell -ExecutionPolicy Bypass -File run_local.ps1 3011
```

- Dashboard: http://localhost:3011
- `/health`, `/spark`, `/kafka`: estado JSON.
- Con el broker en `localhost:9092` el badge Kafka queda **live** (`produced`≈`ingested`).
  Sin broker cae a **fallback** (buffer en memoria) y se **reconecta solo** cuando el broker
  vuelve (ver `server_producer._maybe_reconnect`).

## Correr en Docker (clúster Spark + Kafka interno)

```bash
docker compose up -d --build
```

El contenedor `telemetry-app` (http://localhost:3003) se une a:
- `taller-123_default`: red del clúster Spark (`spark-master:7077`).
- `act1234_default`: red de Kafka (`act1234-kafka-1:9093`, listener INTERNAL).

> El broker DECLARA el listener INTERNAL en `act1234/docker-compose.yml`. Si llega a
> recrearse y crashea con `NodeExistsException` (znode viejo en Zookeeper), esperar
> ~60 s a que expire la sesión y `docker start act1234-kafka-1`.

## Estructura

```
app.py              # FastAPI + python-socketio + SparkSession + pipeline loop
producer.py         # Producer (asyncio): simula clientes emitiendo telemetría
server_producer.py  # Producer #2 (Kafka): servidores de juego → tópico Kafka (auto-reconnect)
kafka_ingest.py     # Thread que lee el tópico Kafka y alimenta al consumer
consumer.py         # Consumer con Spark: lag() + groupBy + row_number()
state.py            # GameState: jugadores, matches, servidores, alertas
models.py           # Dataclasses
templates/index.html# Dashboard (HTML + JS + Tailwind CDN + Chart.js)
Dockerfile / docker-compose.yml / run_local.ps1 / requirements.txt
```

## Endpoints

| Ruta | Descripción |
|---|---|
| `GET /` | Dashboard HTML |
| `GET /health` | Conteos (players, matches, servers, alerts) |
| `GET /spark` | Estado de Spark (versión, batches, pipelineAlive) |
| `GET /kafka` | Estado del bus Kafka (broker, tópico, produced/ingested, live/fallback) |