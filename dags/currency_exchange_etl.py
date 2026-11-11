"""
Currency exchange ETL: Frankfurter API -> transform -> MongoDB.
Three tasks linked with named XComs (explicit keys).
"""
from datetime import datetime, timedelta, timezone

try:
    from airflow.sdk import dag, task            # Airflow 3 (newer Astro Runtime)
except ImportError:
    from airflow.decorators import dag, task     # Airflow 2

from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mongo.hooks.mongo import MongoHook

# ---- Config -----------------------------------------------------------
API_CONN_ID = "frankfurter_api"      # HTTP connection (host: https://api.frankfurter.dev)
MONGO_CONN_ID = "mongo_default"      # Mongo connection (host: mongo)
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = "USD,GBP,JPY"
MONGO_DB = "currency_db"
MONGO_COLLECTION = "exchange_rates"

# Named XCom keys
RAW_KEY = "raw_exchange_rates"
TRANSFORMED_KEY = "transformed_exchange_rates"


@dag(
    dag_id="currency_exchange_etl",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["etl", "currency", "mongodb"],
)
def currency_exchange_etl():

    # 1) EXTRACT ----------------------------------------------------------
    @task(task_id="extract_exchange_rates")
    def extract_exchange_rates(**context):
        ti = context["ti"]

        hook = HttpHook(http_conn_id=API_CONN_ID, method="GET")
        response = hook.run(
            endpoint="/v2/rates",
            data={"base": BASE_CURRENCY, "quotes": TARGET_CURRENCIES},
        )
        raw = response.json()   # list of {"date", "base", "quote", "rate"}

        if not raw:
            raise ValueError("Frankfurter API returned no rates")

        # Named XCom: push under an explicit key
        ti.xcom_push(key=RAW_KEY, value=raw)

    # 2) TRANSFORM --------------------------------------------------------
    @task(task_id="transform_exchange_rates")
    def transform_exchange_rates(**context):
        ti = context["ti"]

        raw = ti.xcom_pull(task_ids="extract_exchange_rates", key=RAW_KEY)
        retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

        documents = [
            {
                "base_currency": item["base"],
                "target_currency": item["quote"],
                "exchange_rate": float(item["rate"]),
                "rate_date": item["date"],
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at,
            }
            for item in raw
        ]

        ti.xcom_push(key=TRANSFORMED_KEY, value=documents)

    # 3) LOAD -------------------------------------------------------------
    @task(task_id="load_exchange_rates")
    def load_exchange_rates(**context):
        ti = context["ti"]

        documents = ti.xcom_pull(
            task_ids="transform_exchange_rates", key=TRANSFORMED_KEY
        )

        hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
        collection = hook.get_collection(MONGO_COLLECTION, mongo_db=MONGO_DB)

        # Upsert: one document per (base, target, date) so reruns don't duplicate
        for doc in documents:
            collection.replace_one(
                {
                    "base_currency": doc["base_currency"],
                    "target_currency": doc["target_currency"],
                    "rate_date": doc["rate_date"],
                },
                doc,
                upsert=True,
            )

    extract_exchange_rates() >> transform_exchange_rates() >> load_exchange_rates()


currency_exchange_etl()
