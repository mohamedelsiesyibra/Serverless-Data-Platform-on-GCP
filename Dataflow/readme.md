In order to run this file you will have to use the cloud shell CLI:

First, create a gcs bucket with two folders [temp, staging], and upload the .py file and then run the following commands using the CLI.

## Create and activate virtual environment
sudo apt-get install -y python3-venv
python3 -m venv beam-env
source beam-env/bin/activate

## Install the required packages
python3 -m pip install -q --upgrade pip setuptools wheel
python3 -m pip install apache-beam[gcp]

## Enable the dataflow API
gcloud services enable dataflow.googleapis.com

## CLI pipeline excution
export PROJECT_ID=$(gcloud config get-value project)
export REGION='us-central1'
export BUCKET=gs://${PROJECT_ID}
export PIPELINE_FOLDER=${BUCKET}
export RUNNER=DataflowRunner
export INPUT__TOPIC=projects/${PROJECT_ID}/topics/enter-your-topic-name
export OUTPUT__TOPIC=projects/${PROJECT_ID}/topics/enter-your-sub-name
export BIGTABLE_INSTANCE_ID=enter_your_instance_id
export BIGTABLE_TABLE_ID=enter_your_table_id
python3 enter_your_script_name.py \
--project=${PROJECT_ID} \
--region=${REGION} \
--staging_location=${PIPELINE_FOLDER}/staging \
--temp_location=${PIPELINE_FOLDER}/temp \
--runner=${RUNNER} \
--input_topic=${INPUT__TOPIC} \
--output_topic=${OUTPUT__TOPIC} \
--bigtable_instance_id=${}BIGTABLE_INSTANCE_ID \
--bigtable_table_id=${BIGTABLE_TABLE_ID}