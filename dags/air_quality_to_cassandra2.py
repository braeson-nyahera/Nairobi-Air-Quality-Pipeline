from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timedelta

import requests
from airflow.sdk import dag, task
from cassandra.cluster import Cluster

logger = logging.getLogger(__name__)

API_URL = "https://api.openaq.org/v3"
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "air_quality")
API_KEY = os.getenv("OPENAQ_API_KEY") or os.getenv("OpenAQ_API_KEY")
PAGE_SIZE = 100
REQUEST_DELAY_SECONDS = 0.25  # be polite to the OpenAQ rate limiter

_KEYSPACE_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
if not _KEYSPACE_RE.match(CASSANDRA_KEYSPACE):
    raise ValueError(
        f"CASSANDRA_KEYSPACE {CASSANDRA_KEYSPACE!r} is not a valid Cassandra identifier"
    )


def _connect_to_cassandra(max_retries=20, delay_seconds=5):
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                "Connecting to Cassandra at %s:%s (attempt %s/%s)",
                CASSANDRA_HOST,
                CASSANDRA_PORT,
                attempt,
                max_retries,
            )
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, connect_timeout=10)
            session = cluster.connect()
            return cluster, session
        except Exception as exc:  # pragma: no cover - depends on runtime startup timing
            last_error = exc
            logger.warning("Cassandra connection attempt %s failed: %s", attempt, exc)
            if attempt == max_retries:
                raise
            time.sleep(delay_seconds)
    raise last_error


def _headers() -> dict:
    if not API_KEY:
        raise RuntimeError(
            "OpenAQ API key is missing. Set the environment variable OPENAQ_API_KEY or OpenAQ_API_KEY."
        )
    return {"X-API-Key": API_KEY}


def _paginated_get(url: str, headers: dict, params: dict | None = None) -> list[dict]:
    """Fetch every page of results from an OpenAQ v3 endpoint."""
    all_results: list[dict] = []
    page = 1
    params = dict(params or {})
    params["limit"] = PAGE_SIZE

    while True:
        params["page"] = page
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()

        payload = response.json()
        results = payload.get("results", [])
        all_results.extend(results)

        found = payload.get("meta", {}).get("found")
        if len(results) < PAGE_SIZE:
            break
        if isinstance(found, int) and len(all_results) >= found:
            break

        page += 1
        time.sleep(REQUEST_DELAY_SECONDS)

    return all_results


def _dt_pair(obj: dict | None) -> tuple[str | None, str | None]:
    """Split an OpenAQ {"utc": ..., "local": ...} datetime object into a tuple."""
    if not obj:
        return None, None
    return obj.get("utc"), obj.get("local")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="air_quality_to_cassandra2",
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
        headers = _headers()
        params = {"coordinates": "-1.286389,36.817223", "radius": 25000}
        records = _paginated_get(f"{API_URL}/locations", headers, params)
        logger.info("No. of locations: %s", len(records))
        return records

    @task
    def extract_sensors(location):
        headers = _headers()
        location_id = location["id"]
        sensors = _paginated_get(f"{API_URL}/locations/{location_id}/sensors", headers)
        for sensor in sensors:
            sensor["locationId"] = location_id
        logger.info("No. of sensors for location %s: %s", location_id, len(sensors))
        return sensors

    @task
    def flatten_sensors(sensors_data):
        return [sensor for sublist in sensors_data for sensor in sublist]

    @task
    def extract_measurements(sensor):
        headers = _headers()
        sensor_id = sensor["id"]
        measurements = _paginated_get(f"{API_URL}/sensors/{sensor_id}/measurements", headers)
        for measurement in measurements:
            measurement["sensorId"] = sensor_id
            measurement["locationId"] = sensor.get("locationId")
        logger.info("No. of measurements for sensor %s: %s", sensor_id, len(measurements))
        return measurements

    @task
    def flatten_measurements(measurements_data):
        return [m for sublist in measurements_data for m in sublist]

    @task
    def write_locations_to_cassandra(records) -> None:
        cluster, session = _connect_to_cassandra()
        try:
            session.set_keyspace(CASSANDRA_KEYSPACE)
            session.execute(
                """
                CREATE TABLE IF NOT EXISTS air_quality_locations (
                    id text PRIMARY KEY,
                    name text,
                    locality text,
                    timezone text,
                    country_code text,
                    country_name text,
                    latitude double,
                    longitude double,
                    is_mobile boolean,
                    is_monitor boolean,
                    first_updated text,
                    last_updated text
                )
                """
            )

            for record in records:
                if not isinstance(record, dict):
                    continue

                coords = record.get("coordinates") or {}
                country = record.get("country") or {}
                first_utc, _ = _dt_pair(record.get("datetimeFirst"))
                last_utc, _ = _dt_pair(record.get("datetimeLast"))

                session.execute(
                    """
                    INSERT INTO air_quality_locations (
                        id, name, locality, timezone, country_code, country_name,
                        latitude, longitude, is_mobile, is_monitor, first_updated, last_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(record.get("id")),
                        record.get("name"),
                        record.get("locality"),
                        record.get("timezone"),
                        country.get("code"),
                        country.get("name"),
                        coords.get("latitude"),
                        coords.get("longitude"),
                        record.get("isMobile"),
                        record.get("isMonitor"),
                        first_utc,
                        last_utc,
                    ),
                )
        finally:
            session.shutdown()
            cluster.shutdown()

    @task
    def write_sensors_to_cassandra(records) -> None:
        cluster, session = _connect_to_cassandra()
        try:
            session.set_keyspace(CASSANDRA_KEYSPACE)
            session.execute(
                """
                CREATE TABLE IF NOT EXISTS air_quality_sensors (
                    id text PRIMARY KEY,
                    location_id text,
                    name text,
                    parameter_name text,
                    parameter_units text,
                    parameter_display_name text,
                    first_updated text,
                    last_updated text
                )
                """
            )

            for record in records:
                if not isinstance(record, dict):
                    continue

                parameter = record.get("parameter") or {}
                first_utc, _ = _dt_pair(record.get("datetimeFirst"))
                last_utc, _ = _dt_pair(record.get("datetimeLast"))

                session.execute(
                    """
                    INSERT INTO air_quality_sensors (
                        id, location_id, name, parameter_name, parameter_units,
                        parameter_display_name, first_updated, last_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(record.get("id")),
                        str(record.get("locationId")) if record.get("locationId") is not None else None,
                        record.get("name"),
                        parameter.get("name"),
                        parameter.get("units"),
                        parameter.get("displayName"),
                        first_utc,
                        last_utc,
                    ),
                )
        finally:
            session.shutdown()
            cluster.shutdown()

    @task
    def write_measurements_to_cassandra(records) -> None:
        cluster, session = _connect_to_cassandra()
        try:
            session.set_keyspace(CASSANDRA_KEYSPACE)
            session.execute(
                """
                CREATE TABLE IF NOT EXISTS air_quality_measurements (
                    sensor_id text,
                    datetime_from_utc text,
                    location_id text,
                    parameter_name text,
                    parameter_units text,
                    value double,
                    datetime_from_local text,
                    datetime_to_utc text,
                    datetime_to_local text,
                    latitude double,
                    longitude double,
                    PRIMARY KEY (sensor_id, datetime_from_utc)
                )
                """
            )

            for record in records:
                if not isinstance(record, dict):
                    continue

                parameter = record.get("parameter") or {}
                period = record.get("period") or {}
                coords = record.get("coordinates") or {}
                from_utc, from_local = _dt_pair(period.get("datetimeFrom"))
                to_utc, to_local = _dt_pair(period.get("datetimeTo"))

                if from_utc is None:
                    # Primary key requires this; skip malformed rows rather than crash the task.
                    logger.warning("Skipping measurement with no period.datetimeFrom: %s", record)
                    continue

                session.execute(
                    """
                    INSERT INTO air_quality_measurements (
                        sensor_id, datetime_from_utc, location_id, parameter_name, parameter_units,
                        value, datetime_from_local, datetime_to_utc, datetime_to_local, latitude, longitude
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(record.get("sensorId")),
                        from_utc,
                        str(record.get("locationId")) if record.get("locationId") is not None else None,
                        parameter.get("name"),
                        parameter.get("units"),
                        record.get("value"),
                        from_local,
                        to_utc,
                        to_local,
                        coords.get("latitude"),
                        coords.get("longitude"),
                    ),
                )
        finally:
            session.shutdown()
            cluster.shutdown()

    # keyspace_task = create_keyspace_if_needed()
    locations_data = extract_locations()
    # keyspace_task >> locations_data

    sensors_data = extract_sensors.expand(location=locations_data)
    flat_sensors_data = flatten_sensors(sensors_data)

    measurements_data = extract_measurements.expand(sensor=flat_sensors_data)
    flat_measurements_data = flatten_measurements(measurements_data)

    write_locations_to_cassandra(locations_data)
    write_sensors_to_cassandra(flat_sensors_data)
    write_measurements_to_cassandra(flat_measurements_data)


air_pollution_pipeline()