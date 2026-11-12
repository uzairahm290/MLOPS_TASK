"""
Currency Exchange Rate ETL pipeline.

Extract  -> pull daily rates from the Frankfurter API
Transform -> reshape each rate into the target document schema
Load     -> upsert the documents into MongoDB

Data moves between tasks through named XComs (explicit keys) rather than
implicit return values.
"""

import os
from datetime import datetime, timedelta

import requests
from airflow.sdk import dag, task
from pymongo import MongoClient, UpdateOne

API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
QUOTE_CURRENCIES = ["USD", "GBP", "JPY", "PKR", "INR", "AUD", "CAD", "CHF", "CNY"]

XCOM_RAW_KEY = "raw_exchange_rates"
XCOM_TRANSFORMED_KEY = "transformed_exchange_rates"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@mongodb:27017/?authSource=admin")
MONGO_DB = os.getenv("MONGO_DB", "currency_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "exchange_rates")

default_args = {
    "owner": "uzair",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="currency_exchange_etl",
    description="Daily EUR exchange rates from Frankfurter API into MongoDB",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "currency", "mongodb"],
)
def currency_exchange_etl():

    @task(task_id="extract_exchange_rates")
    def extract_exchange_rates(ti=None):
        params = {"base": BASE_CURRENCY, "quotes": ",".join(QUOTE_CURRENCIES)}
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        rates = response.json()
        if not rates:
            raise ValueError("Frankfurter API returned no rates")

        print(f"Extracted {len(rates)} rates from {response.url}")
        ti.xcom_push(key=XCOM_RAW_KEY, value=rates)

    @task(task_id="transform_exchange_rates")
    def transform_exchange_rates(ti=None):
        raw_rates = ti.xcom_pull(task_ids="extract_exchange_rates", key=XCOM_RAW_KEY)
        retrieved_at = datetime.now().replace(microsecond=0).isoformat()

        transformed = [
            {
                "base_currency": item["base"],
                "target_currency": item["quote"],
                "exchange_rate": round(float(item["rate"]), 6),
                "rate_date": item["date"],
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at,
            }
            for item in raw_rates
        ]

        for doc in transformed:
            print(doc)
        ti.xcom_push(key=XCOM_TRANSFORMED_KEY, value=transformed)

    @task(task_id="load_to_mongodb")
    def load_to_mongodb(ti=None):
        records = ti.xcom_pull(task_ids="transform_exchange_rates", key=XCOM_TRANSFORMED_KEY)
        if not records:
            raise ValueError("No transformed records found in XCom")

        # Upsert on (base, target, date) so re-runs for the same day don't duplicate rows
        operations = [
            UpdateOne(
                {
                    "base_currency": r["base_currency"],
                    "target_currency": r["target_currency"],
                    "rate_date": r["rate_date"],
                },
                {"$set": r},
                upsert=True,
            )
            for r in records
        ]

        with MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000) as client:
            collection = client[MONGO_DB][MONGO_COLLECTION]
            collection.create_index(
                [("base_currency", 1), ("target_currency", 1), ("rate_date", 1)],
                unique=True,
            )
            result = collection.bulk_write(operations)
            print(
                f"MongoDB {MONGO_DB}.{MONGO_COLLECTION}: "
                f"upserted={result.upserted_count}, modified={result.modified_count}, "
                f"total_docs={collection.count_documents({})}"
            )

    extract_exchange_rates() >> transform_exchange_rates() >> load_to_mongodb()


currency_exchange_etl()
