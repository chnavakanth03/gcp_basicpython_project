
# Importing required libraries
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, PreconditionFailed
import os

# Variables section
PROJECT_ID = "gcp-raju-53-prod"
BUCKET_NAME = "gcp-raju-53-prod-bucket-e10_new2"
LOCAL_FILE_PATH = r'C:\\gcp_de_56\\gcp_python_project\\customer_transactions.csv'
GCS_FILE_NAME = 'customers_daily_txns.csv'
DATASET_ID = 'CUSTOMER_TRANSACTIONS_3'
TABLE_ID = 'customers_txns_t'
QUERY = f'SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`'

#LOCAL_FILE_PATH = r'C:\\GCP-DE-B53\\gcp_python_project\\customers_trxns.csv'


# GCS bucket creation task
def create_bucket_class_location_task(project_id,bucket_name):

    """
    Create a GCS bucket in the US region
    with STANDARD storage class.
    """
    storage_client = storage.Client(project=PROJECT_ID)

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "STANDARD"

    new_bucket = storage_client.create_bucket(
        bucket,
        location="us-central1"
    )

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name,
            new_bucket.location,
            new_bucket.storage_class
        )
    )


# Local to GCS upload task
#def local_to_gcs_upload_task():
#    print("Local to GCS upload task started")
#    pass

def upload_blob_from_local_task(project_id, bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    bucket_name = bucket_name
    # The path to your file to upload
    source_file_name = source_file_name
    # The ID of your GCS object
    destination_blob_name = destination_blob_name

    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    generation_match_precondition = 0

    blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

# bq_dataset_creation_task 
def bq_dataset_creation_task(project_id,datasetid): 
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_id)
    
    # TODO(developer): Set dataset_id to the ID of the dataset to create.
    dataset_id = "{}.{}".format(client.project,datasetid) #project_id.dataset_id
    
    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    
    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location = "us-central1"
    
    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))



# BigQuery dataset creation task
def bq_dataset_creation_task(project_id,datasetid): 
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_id)
    
    # TODO(developer): Set dataset_id to the ID of the dataset to create.
    dataset_id = "{}.{}".format(client.project,datasetid) #project_id.dataset_id
    
    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    
    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location = "us-central1"
    
    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))


# GCS to BigQuery load task
#def gcs_to_bq_load_task():
 #   print("GCS to BigQuery load task started")
 #   pass

def gcs_to_bq_load_task(project_id,dataset_id,table_name,bucket_name,blob_name): 
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_id)
    
    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "{}.{}.{}".format(project_id,dataset_id,table_name)
    
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    uri = "gs://{}/{}".format(bucket_name,blob_name)
    
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.
    
    load_job.result()  # Waits for the job to complete.
    
    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))


# Data validation task
def data_validation_task():
    print("Data validation task started")
    pass


# Main function
def main():
    print("Welcome to Python")
    print("Welcome to GCP Data Engineering")

    print("GCP Data Pipeline started execution...")

    #create_bucket_class_location_task(BUCKET_NAME)
    #create_bucket_class_location_task(PROJECT_ID,BUCKET_NAME)
    #upload_blob_from_local_task(PROJECT_ID,BUCKET_NAME,LOCAL_FILE_PATH,GCS_FILE_NAME)
   #bq_dataset_creation_task(PROJECT_ID,DATASET_ID)
    #gcs_to_bq_load_task(PROJECT_ID,DATASET_ID,TABLE_ID,BUCKET_NAME,GCS_FILE_NAME)
# data_validation_task 
def data_validation_task(project_id,query): 
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_id)
    
    query =  query
    rows = client.query_and_wait(query)  # Make an API request.
    
    print("The query data:")
    for row in rows:
        # Row values can be accessed by field name or index.
        print("your table has got rows...", row[0])

if __name__ == "__main__":
    print("This is a GCP DE EL Data Pipelinie. It started executing......")
    create_bucket_class_location_task(PROJECT_ID,BUCKET_NAME) # we have to develop this task first 
    upload_blob_from_local_task(PROJECT_ID,BUCKET_NAME,LOCAL_FILE_PATH,GCS_FILE_NAME)
    bq_dataset_creation_task(PROJECT_ID,DATASET_ID)
    gcs_to_bq_load_task(PROJECT_ID,DATASET_ID,TABLE_ID,BUCKET_NAME,GCS_FILE_NAME)
    data_validation_task(PROJECT_ID,QUERY)
    print("The pipeline is finished Successfully")
