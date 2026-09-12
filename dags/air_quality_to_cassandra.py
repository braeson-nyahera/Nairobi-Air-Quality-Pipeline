from __future__ import annotations

import os
import time
import requests
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from cassandra.cluster import Cluster

API_URL = "https://api.openaq.org/v3"
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "air_quality")
API_KEY = os.getenv("OPENAQ_API_KEY") or os.getenv("OpenAQ_API_KEY")


def _connect_to_cassandra(max_retries=20, delay_seconds=5):
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, connect_timeout=10)
            session = cluster.connect()
            return cluster, session
        except Exception as exc:  # pragma: no cover - depends on runtime startup timing
            last_error = exc
            if attempt == max_retries:
                raise
            time.sleep(delay_seconds)
    raise last_error


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="air_quality_to_cassandra",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["air-quality", "cassandra"],
)
def air_pollution_pipeline():
    # @task
    # def create_keyspace_if_needed() -> None:
    #     cluster, session = _connect_to_cassandra()
    #     try:
    #         query = (
    #             "CREATE KEYSPACE IF NOT EXISTS "
    #             f"{CASSANDRA_KEYSPACE} "
    #             "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
    #         )
    #         session.execute(query)
    #     finally:
    #         session.shutdown()
    #         cluster.shutdown()

    @task
    def extract_locations():
        if not API_KEY:
            raise RuntimeError(
                "OpenAQ API key is missing. Set the environment variable OPENAQ_API_KEY or OpenAQ_API_KEY."
            )

        headers = {"X-API-Key": API_KEY}
        params = {
            "coordinates": "-1.286389,36.817223",
            "radius": 25000,
            "limit": 100,
        }

        response = requests.get(f"{API_URL}/locations", headers=headers, params=params, timeout=30)
        response.raise_for_status()

        records = response.json().get("results", [])
        print("No. of locations:", len(records))
        return records

    @task
    def extract_sensors(location):
        if not API_KEY:
            raise RuntimeError(
                "OpenAQ API key is missing. Set the environment variable OPENAQ_API_KEY or OpenAQ_API_KEY."
            )

        location_id = location["id"]
        headers = {"X-API-Key": API_KEY}

        response = requests.get(f"{API_URL}/locations/{location_id}/sensors", headers=headers, timeout=30)
        response.raise_for_status()

        sensors = response.json().get("results", [])
        print("No. of sensors:", len(sensors))
        return sensors

    @task
    def extract_measurements(sensor):
        if not API_KEY:
            raise RuntimeError(
                "OpenAQ API key is missing. Set the environment variable OPENAQ_API_KEY or OpenAQ_API_KEY."
            )

        sensor_id = sensor["id"]
        headers = {"X-API-Key": API_KEY}

        response = requests.get(
            f"{API_URL}/sensors/{sensor_id}/measurements",
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()

        measurements = response.json().get("results", [])
        print("No. of measurements:", len(measurements))
        return measurements

    @task
    def create_data_for_db(locations_data, sensors_data, measurements_data):
        flat_sensors = [item for sublist in sensors_data for item in sublist]
        flat_measurements = [item for sublist in measurements_data for item in sublist]
        return locations_data + flat_sensors + flat_measurements

    @task
    def write_to_cassandra(records) -> None:
        cluster, session = _connect_to_cassandra()
        try:
            session.set_keyspace(CASSANDRA_KEYSPACE)
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
                if not isinstance(record, dict):
                    continue

                coords = record.get("coordinates", {})
                session.execute(
                    """
                    INSERT INTO air_quality_locations (
                        id, city, country, timezone, name, latitude, longitude, first_updated, last_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        record.get("id"),
                        record.get("city"),
                        record.get("country"),
                        record.get("timezone"),
                        record.get("location"),
                        coords.get("latitude"),
                        coords.get("longitude"),
                        record.get("firstUpdated"),
                        record.get("lastUpdated"),
                    ),
                )
        finally:
            session.shutdown()
            cluster.shutdown()

    # keyspace_task = create_keyspace_if_needed()
    locations_data = extract_locations()
    # keyspace_task >> locations_data

    sensors_data = extract_sensors.expand(location=locations_data)
    measurements_data = extract_measurements.expand(sensor=sensors_data)
    final_data = create_data_for_db(locations_data, sensors_data, measurements_data)
    write_to_cassandra(final_data)


air_pollution_pipeline()
