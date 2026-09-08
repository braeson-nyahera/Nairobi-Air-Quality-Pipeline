from __future__ import annotations

import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement


API_URL = "https://api.openaq.org/v3/locations"
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "air_quality")


def create_keyspace_if_needed() -> None:
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect()
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE} "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
    )
    session.shutdown()
    cluster.shutdown()


def fetch_air_quality_data() -> list[dict]:
    params = {
        "coordinates": "-1.286389,36.817223",
        "radius": 25000,
        "limit": 100,
    }
    headers = {"X-API-Key": os.getenv("OpenAQ_API_KEY", "")}

    response = requests.get(API_URL, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    results = data.get("results", [])

    return results


def write_to_cassandra(**context) -> None:
    records = context["task_instance"].xcom_pull(task_ids="extract_air_quality")

    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS air_quality_locations (
            id text,
            city text,
            country text,
            timezone text,
            name text,
            latitude double,
            longitude double,
            first_updated text,
            last_updated text,
            PRIMARY KEY (id, name)
        )
        """
    )

    for record in records:
        location = record.get("location")
        city = record.get("city")
        country = record.get("country")
        timezone = record.get("timezone")
        coordinates = record.get("coordinates", {})

        session.execute(
            """
            INSERT INTO air_quality_locations (
                id, city, country, timezone, name, latitude, longitude, first_updated, last_updated
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                record.get("id"),
                city,
                country,
                timezone,
                location,
                coordinates.get("latitude"),
                coordinates.get("longitude"),
                record.get("firstUpdated"),
                record.get("lastUpdated"),
            ),
        )

    cluster.shutdown()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="air_quality_to_cassandra",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["air-quality", "cassandra"],
) as dag:

    create_cassandra_keyspace = PythonOperator(
        task_id="create_cassandra_keyspace",
        python_callable=create_keyspace_if_needed,
    )

    extract_air_quality = PythonOperator(
        task_id="extract_air_quality",
        python_callable=fetch_air_quality_data,
    )

    write_air_quality_to_cassandra = PythonOperator(
        task_id="write_air_quality_to_cassandra",
        python_callable=write_to_cassandra,
    )

    create_cassandra_keyspace >> extract_air_quality >> write_air_quality_to_cassandra
