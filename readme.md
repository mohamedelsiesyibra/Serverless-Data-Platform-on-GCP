# Salads-XYZ Data Platform

Salads-XYZ is an imaginary, fast-growing, meal-kit company that specializes in selling fresh salads. As the company is growing rapidly, we have decided to build our data platform on Google Cloud.

Our infrastructure includes one main internal kitchen and eight distributed stores around the city. We have two sources of data:

1. An **API endpoint** that streams inventory data for all stores.
2. A **PostgreSQL database** contains all the sales data.

## Proposed Solutions

We are looking to use GCP to develop two solutions:

1. **Real-Time Alerting Pipeline:** Ingest streaming inventory data in real-time and send an alert once any of our products is out of stock.
2. **Sales Forecast Report:** A daily report using machine learning to predict the number of sales for each product using our sales historical data and weather data from OpenWeather.

## Tech Stack

Our chosen technologies are as follows:

- **Languages:** SQL, Python.
- **GCP Services:** BigQuery, Vertex AI (Auto-ML), DataFlow, PubSub, Cloud Functions, Cloud Logging, Cloud Scheduler, BigTable, DataStream.
- **Third Party Services:** Twilio, OpenWeather.

## Architecture

The architecture of our solutions is represented in the image below:

![ETL Architecture](Images/ETL-Architecture.png)
