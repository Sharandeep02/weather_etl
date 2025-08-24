
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task
# from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import json
from airflow.providers.http.hooks.http import HttpHook

# Latitude and longitude of the location(London)
LATITUDE = 51.5074
LONGITUDE = -0.1278 # London

# LATITUDE = 12.9629
# LONGITUDE = 77.5676 # Bangalore

POSTGRES_CONN_ID = "postgres_default"
# API key for OpenWeatherMap
API_CONN_ID = "open_meteo_api"

# URL for the OpenWeatherMap API
# url = f"https://api.openweathermap.org/data/2.5/weather?lat={LATITUDE}&lon={LONGITUDE}&appid={API_CONN_ID}"
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 24)
}
# DAG object
with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=["weather"],
) as dag:

    # Define tasks
    @task
    def extract_data():
        """Extract data from the Meteo API from Airflow connection."""

        #Use the HttpHook to get the data from the API
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method="GET")

        #Build the URL
        #https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint = f"v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"

        #Make the API request
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data from the API. Status code: {response.status_code}")

    @task
    def transform_weather(data):
        """Transform the data from the API to a pandas dataframe."""
        current_weather = data["current_weather"]
        transform_data = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "temperature": current_weather["temperature"],
            "windspeed": current_weather["windspeed"],
            "winddirection": current_weather["winddirection"],
            "weathercode": current_weather["weathercode"]
        }
        return transform_data

    @task
    def load_weather_data(transform_data):
        """Load the data into the Postgres database."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        #create a table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            is_day BOOLEAN,
            time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        #Insert the transformed data into the table
        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transform_data["latitude"], 
            transform_data["longitude"], 
            transform_data["temperature"], 
            transform_data["windspeed"], 
            transform_data["winddirection"], 
            transform_data["weathercode"]
        ))
        
        # Commit the transaction
        conn.commit()
        cursor.close()
        conn.close()

    # DAG Workflow ETL pipeline
    weather_data = extract_data()
    transform_data = transform_weather(weather_data)
    load_weather_data(transform_data)

    # Define the task dependencies
    # weather_data >> transform_data >> load_weather_data