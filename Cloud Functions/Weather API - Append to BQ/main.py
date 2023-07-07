import requests
import pandas as pd
import pandas_gbq
from datetime import datetime, timedelta
import google.auth
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os

def write_file_to_bq(event, context):
    # Request the data from the OpenWeather API.
    url = "http://api.openweathermap.org/data/2.5/forecast?q=London&appid=" + os.getenv("OPENWEATHER_API_KEY")
    
    try:
        response = requests.get(url)
        data = response.json()
    except Exception as e:
        print(f"Error getting data from OpenWeather API: {e}")
        return

    # Get the tomorrow date.
    tomorrow = datetime.now() + timedelta(days=1)
    tomorrow_mid_day = tomorrow.strftime('%Y-%m-%d') + ' 12:00:00'
    
    # Create the main dataframe.
    df = pd.DataFrame(data['list'])
    
    # Create the new columns.
    df['date'] = df['dt_txt'].str.split().str[0]
    df['weather'] = [i['main'] for i in df['weather'][0]][0]
    df['inserted_at'] = datetime.now()

    # Filter the dataframe.
    df = df.loc[df['dt_txt'] == tomorrow_mid_day]
    
    # Cretae the final table.
    weather_table = df[['date', 'weather', 'inserted_at']]

    # Create a BigQuery client.
    client = bigquery.Client()

    # Set the BigQuery project ID, dataset, table name and table id.
    _, project_id = google.auth.default()
    dataset_name = "sales"
    table_name = "daily_weather_table"
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    # Check if the table exists in bigquery.
    try:
        client.get_table(table_id)  # Will raise NotFound if table does not exist.
        pandas_gbq.to_gbq(weather_table, table_id, if_exists='append')
        print("weather_table_updated")
    except NotFound:
        print("Table not found, creating it.")
        pandas_gbq.to_gbq(weather_table, table_id, if_exists='fail')
        print("weather_table_updated")
    except Exception as e:
        print(f"Error occurred while writing to BigQuery: {e}")
