# Currency Exchange ETL (Astro CLI + Airflow + MongoDB)

Daily ETL pipeline that pulls EUR exchange rates from the [Frankfurter API](https://frankfurter.dev/), transforms them and loads them into MongoDB. Mongo Express runs alongside as the web admin UI.

## Pipeline

`extract_exchange_rates` → `transform_exchange_rates` → `load_to_mongodb`

Data moves between tasks through **named XComs**:

| Key | Pushed by | Pulled by |
|-----|-----------|-----------|
| `raw_exchange_rates` | extract_exchange_rates | transform_exchange_rates |
| `transformed_exchange_rates` | transform_exchange_rates | load_to_mongodb |

Each document loaded into `currency_db.exchange_rates`:

```json
{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.1134,
  "rate_date": "2026-11-12",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-12T19:40:00"
}
```

## Docker setup (`docker-compose.override.yml`)

- **mongodb** (`mongo:latest`): port 27017, healthcheck
- **mongo-express** (`mongo-express:1.0.2`): http://localhost:8081 (admin / admin123)
- **Networking:** custom bridge network `currency_etl_network`; MongoDB is also attached to the Astro `airflow` network so the scheduler reaches it at `mongodb:27017`
- **Persistent storage:** named volumes `currency_mongo_data` (`/data/db`) and `currency_mongo_config`
- **Metadata:** `LABEL`s on the Airflow image (Dockerfile) and `labels` on the services, network and volumes

## Run

```bash
cp .env.example .env
astro dev start
# Airflow UI: http://localhost:8080   Mongo Express: http://localhost:8081
astro dev run dags trigger currency_exchange_etl
```
