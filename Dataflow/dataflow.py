import argparse
import time
import logging
import json
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.gcp.bigtableio import WriteToBigTable
from google.cloud.bigtable import row

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json from Pub/Sub into Bigtable')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--staging_location', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--temp_location', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--input_topic', required=True, help='Input Pub/Sub Topic')
    parser.add_argument('--output_topic', required=True, help='Output Pub/Sub Topic')
    parser.add_argument('--bigtable_instance_id', required=True, help='Bigtable instance ID')
    parser.add_argument('--bigtable_table_id', required=True, help='Bigtable table ID')

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(save_main_session=True, streaming=True)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.staging_location
    options.view_as(GoogleCloudOptions).temp_location = opts.temp_location
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('streaming-inventory-logs-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    input_topic = opts.input_topic
    output_topic = opts.output_topic
    bigtable_instance_id = opts.bigtable_instance_id
    bigtable_table_id = opts.bigtable_table_id

    # Filter only the keys inside the single record with the value zero.
    def filter_zero_values(element):
        dict_0 = {key: value for key, value in element.items() if key == "time" or value == 0}
        if len(dict_0) > 1:
            return dict_0

    # Create the pipeline
    with beam.Pipeline(options=options) as p:

        # Read the JSON response from Pub/Sub.
        pipeline = (p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(input_topic)
            | 'Parse JSON' >> beam.Map(lambda x: json.loads(x))
        )

        # Stream the data into the output PubSub topic.
        (pipeline
            | "Filter zero values" >> beam.Map(filter_zero_values)
            | 'Filter None' >> beam.Filter(lambda x: x is not None)
            | "Dumps JSON" >> beam.Map(lambda x: json.dumps(x))
            | "Encode the response" >> beam.Map(lambda x: x.encode("utf-8"))
            | "Write to Pub/Sub" >> beam.io.WriteToPubSub(topic=output_topic)
        )

        # Stream the data into Bigtable
        (pipeline
            | 'Create Mutations' >> beam.Map(lambda x: (x["time"], [row.DirectRow(row_key=str(x["time"])).set_cell('cf', k, v) for k, v in x.items() if k != "time"]))
            | 'WriteToBigTable' >> WriteToBigTable(
                project_id=opts.project,
                instance_id=bigtable_instance_id,
                table_id=bigtable_table_id
            )
        )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()

if __name__ == '__main__':
  run()
