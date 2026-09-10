#from __future__ import annotations

import os
from datetime import datetime, timedelta

import requests
from airflow import dag, task
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement


API_URL = "https://api.openaq.org/v3/locations"
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "air_quality")

API_KEY = os.getenv('OpenAQ_API_KEY')


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
    schedule_interval="@daily",
    catchup=False,
    tags=["air-quality", "cassandra"],
)
def air_pollution_pipeline():
    location_ids = []
    sensors = []

    @task
    def create_keyspace_if_needed() -> None:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE} "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
        )
        session.shutdown()
        cluster.shutdown()

    @task
    def extract_locations():
        headers = {
            "X-API-Key": API_KEY,
            }
        params = {
            "coordinates":  "-1.286389,36.817223",
            "radius": 25000,
            "limit": 100
            }
         
        

        response = requests.get(API_URL, headers=headers, params=params)
        response.raise_for_status()

        records = response.json()['results']

        return records

    @task
    def extract_sensors(location):

        location_id = location['id']
        headers = {
            "X-API-Key": API_KEY,
            }
            

        response = requests.get(f"{API_URL}/{location_id}/sensors", headers=headers)
        response.raise_for_status()

        sensors = response.json()['results']

        return sensors
    
    @task
    def extract_measurements(sensor):

        sensor_id = sensor['id']

        headers = {
            "X-API-Key": API_KEY,
            }
            
        response = requests.get(f"{API_URL}sensors/{sensor_id}/measurements", headers=headers)
        response.raise_for_status()

        return response.json()

    @task 
    def create_data_for_db(locations_data, sensors_data, measurements_data):
        list_data = locations_data + sensors_data + measurements_data

        return list_data
    
    @task
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

    locations_data = extract_locations()

    sensors_data = extract_sensors.expand(location=locations_data)
    measurements_data = extract_measurements.expand(sensor = sensors_data)

    data_for_db = create_data_for_db(locations_data, sensors_data, measurements_data)

air_pollution_pipeline()
