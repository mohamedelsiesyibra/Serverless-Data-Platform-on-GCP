from google.cloud import aiplatform_v1beta1
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value

def main(event, context):
    # These variables can be customized as needed
    project_id = "PROJECT_ID"
    display_name = "JOB_NAME"
    model_id = "MODEL_ID"
    instances_format = "bigquery"
    input_table = "DATASET_NAME.TABLE_NAME"
    predictions_format = "bigquery"
    output_dataset = "DATASET_NAME"
    location = "us-central1" 
    api_endpoint = "us-central1-aiplatform.googleapis.com" 

    # Model name according to required format
    model_name = f'projects/{project_id}/locations/{location}/models/{model_id}'
    
    # Input and output URIs for BigQuery
    bigquery_source_input_uri = f'bq://{project_id}.{input_table}'
    bigquery_destination_output_uri = f'bq://{project_id}.{output_dataset}'

    # Create the AI Platform client.
    client_options = {"api_endpoint": api_endpoint}
    client = aiplatform_v1beta1.JobServiceClient(client_options=client_options)
    model_parameters_dict = {}
    model_parameters = json_format.ParseDict(model_parameters_dict, Value())

    # Define the job arg
    batch_prediction_job = {
        "display_name": display_name,
        "model": model_name,
        "model_parameters": model_parameters,
        "input_config": {
            "instances_format": instances_format,
            "bigquery_source": {"input_uri": bigquery_source_input_uri},
        },
        "output_config": {
            "predictions_format": predictions_format,
            "bigquery_destination": {"output_uri": bigquery_destination_output_uri},
        },
        "generate_explanation": True,
    }
    
    # RUN
    parent = f"projects/{project_id}/locations/{location}"
    response = client.create_batch_prediction_job(
        parent=parent, batch_prediction_job=batch_prediction_job
    )
    
    print("response:", response)

