from google.cloud import bigquery
from twilio.rest import Client
from datetime import datetime
import pandas
import os

def send_sms_with_prediction(event, context):
    project_id = os.getenv("BIGQUERY_PROJECT_ID")
    dataset_name = "sales"
    twilio_sid = os.getenv("TWILIO_SID")
    twilio_auth_token = os.getenv("TWILIO_AUTH_TOKEN")

    bigquery_client = bigquery.Client(project=project_id)
    dataset = bigquery_client.dataset(dataset_name)

    today = datetime.today().strftime('%Y_%m_%d')
    tables = list(bigquery_client.list_tables(dataset))

    table_name = None
    for table in tables:
        if table.table_id.startswith("predictions_" + today):
            table_name = table.table_id
            break

    if table_name is None:
        print(f"No table found for today's date: {today}")
        return

    query = f"""
    SELECT product_name, round(predicted_total_sales.value) as value 
    FROM `{project_id}.{dataset_name}.{table_name}`
    """
    
    try:
        df = bigquery_client.query(query).to_dataframe()
    except Exception as e:
        print(f"Error executing BigQuery: {e}")
        return

    if df.empty:
        print("No data found in the table")
        return

    client = Client(twilio_sid, twilio_auth_token)

    sms_body = '\n'.join(df.apply(lambda row: f"product_name: {row['product_name']} | predicted_total_sales: {row['value']}", axis=1))

    try:
        client.messages.create(
            to=os.getenv("TWILIO_TO"),
            from_=os.getenv("TWILIO_FROM"),
            body=sms_body,
        )
        print("SMS sent!")
    except Exception as e:
        print(f"Error sending SMS: {e}")
