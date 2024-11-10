from pymongo import MongoClient
from google.cloud import bigquery
import json
import math
from bson import ObjectId
from datetime import datetime
import os
from tqdm import tqdm


# Function to convert MongoDB documents to a JSON-serializable format
def convert_to_serializable(doc):
    if '_id' in doc and isinstance(doc['_id'], ObjectId):
        doc['_id'] = str(doc['_id'])  # Convert ObjectId to string

    if 'date_' in doc and isinstance(doc['date_'], str):
        try:
            doc['date_'] = datetime.strptime(doc['date_'], '%Y-%m-%d').date().isoformat()
        except ValueError:
            doc['date_'] = None

    return doc


# BigQuery connection with credentials
try:
    bq_client = bigquery.Client.from_service_account_json('skintific-data-warehouse-ea77119e2e7a.json')
except Exception as e:
    print("Error connecting to BigQuery:", e)
    exit(1)

# Define dataset and table IDs
dataset_id = 'sadata'
original_table_id = 'so_data_mongo'
temp_table_id = 'so_data_mongo_temp'


# Check if the table exists and create it if not
def create_table_if_not_exists(client, dataset_id, table_id):
    table_ref = client.dataset(dataset_id).table(table_id)
    try:
        client.get_table(table_ref)  # Check if the table exists
        print(f"Table {dataset_id}.{table_id} already exists.")
    except Exception:
        print(f"Table {dataset_id}.{table_id} not found. Creating new table...")
        schema = [
            bigquery.SchemaField('_id', 'STRING'),
            bigquery.SchemaField('month_name', 'STRING'),
            bigquery.SchemaField('date_', 'DATE'),
            bigquery.SchemaField('channel_name', 'STRING'),
            bigquery.SchemaField('account_name', 'STRING'),
            bigquery.SchemaField('store_code', 'STRING'),
            bigquery.SchemaField('store_name', 'STRING'),
            bigquery.SchemaField('employee_nik', 'STRING'),
            bigquery.SchemaField('employee_name', 'STRING'),
            bigquery.SchemaField('position_name', 'STRING'),
            bigquery.SchemaField('agency_name', 'STRING'),
            bigquery.SchemaField('leader', 'STRING'),
            bigquery.SchemaField('cluster_name', 'STRING'),
            bigquery.SchemaField('product_brand_name', 'STRING'),
            bigquery.SchemaField('product_code', 'STRING'),
            bigquery.SchemaField('product_name', 'STRING'),
            bigquery.SchemaField('qty', 'INTEGER'),
            bigquery.SchemaField('price', 'FLOAT'),
            bigquery.SchemaField('value', 'FLOAT')
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Table {table_id} created successfully.")


# Ensure both original and temporary tables exist before loading data
create_table_if_not_exists(bq_client, dataset_id, temp_table_id)
create_table_if_not_exists(bq_client, dataset_id, original_table_id)


# MongoDB connection
try:
    mongo_client = MongoClient(
        'mongodb://skintific_ra:Keez3zzPM0tnWaZk@152.42.161.121:8527/skintific?authSource=skintific&authMechanism=SCRAM-SHA-256')
    db = mongo_client['skintific']
    collection = db['sell_outs']
except Exception as e:
    print("Error connecting to MongoDB:", e)
    exit(1)

# Define date ranges and channel names
date_ranges = [
    ("2024-11-01", "2024-11-07"),
    ("2024-11-08", "2024-11-14"),
    ("2024-11-15", "2024-11-21"),
    ("2024-11-22", "2024-11-31"),
]
channel_name = ['GT', 'MTI', 'OFFICE']

# MongoDB projection with updated columns
projection = {
    '_id': 1,
    'month_name': 1,
    'date_': 1,
    'channel_name': 1,
    'account_name': 1,
    'store_code': 1,
    'store_name': 1,
    'employee_nik': 1,
    'employee_name': 1,
    'position_name': 1,
    'agency_name': 1,
    'leader': 1,
    'cluster_name': 1,
    'product_brand_name': 1,
    'product_code': 1,
    'product_name': 1,
    'qty': 1,
    'price': 1,
    'value': 1
}

# Retrieve and filter data from MongoDB for each date range
all_data = []
for start_date, end_date in date_ranges:
    query = {
        'date_': {'$gte': start_date, '$lte': end_date},
        'channel_name': {'$in': channel_name}
    }
    try:
        data = list(collection.find(query, projection))
        print(f"Data count for {start_date} to {end_date}: {len(data)}")
        all_data.extend(data)
    except Exception as e:
        print("Error retrieving data from MongoDB:", e)
        mongo_client.close()
        exit(1)

# Convert MongoDB data for BigQuery insertion and save to local JSON file
formatted_data = [convert_to_serializable(doc) for doc in all_data]
json_file_path = 'mongo_data.json'

# Save the data into a local JSON file with progress bar
with open(json_file_path, 'w') as f:
    for doc in tqdm(formatted_data, desc="Saving MongoDB data to JSON file"):
        f.write(json.dumps(doc) + '\n')
print(f"Filtered data exported to {json_file_path}")


# Load data into the temporary BigQuery table from the JSON file
def load_data_from_file(bq_client, dataset_id, table_id, json_file_path):
    # First, delete any existing data in the temp table to prevent duplicates
    bq_client.query(f"DELETE FROM `{dataset_id}.{table_id}` WHERE TRUE").result()

    table_ref = bq_client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)

    with open(json_file_path, "rb") as source_file:
        load_job = bq_client.load_table_from_file(source_file, table_ref, job_config=job_config)
        print(f"Loading data into BigQuery table: {table_id}")
        load_job.result()  # Wait for the job to complete
        print(f"Loaded {load_job.output_rows} rows into {dataset_id}:{table_id}.")


# Load the data into the temporary table
load_data_from_file(bq_client, dataset_id, temp_table_id, json_file_path)


# Replace the data in the original table with the content from the temporary table
def replace_table_data(bq_client, dataset_id, original_table_id, temp_table_id):
    query = f"""
    CREATE OR REPLACE TABLE `{dataset_id}.{original_table_id}` AS
    SELECT * FROM `{dataset_id}.{temp_table_id}`;
    """
    query_job = bq_client.query(query)
    print(f"Replacing data in {original_table_id} from {temp_table_id}...")
    query_job.result()  # Wait for the job to complete
    print(f"Replaced data in {original_table_id} from {temp_table_id}.")


# Replace the original table data with the data in the temporary table
replace_table_data(bq_client, dataset_id, original_table_id, temp_table_id)

# Close MongoDB connection
mongo_client.close()

# Clean up the local JSON file after upload
if os.path.exists(json_file_path):
    os.remove(json_file_path)
    print(f"Temporary JSON file {json_file_path} removed.")
