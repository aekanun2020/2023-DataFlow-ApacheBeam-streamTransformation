import apache_beam as beam
from datetime import datetime

table_spec = 'bigdatainpractice1:aekanun_dataflow_sql_dataset.aekanun_dfsqltable_sales'
schema = 'tr_time_str:DATETIME, first_name:STRING, last_name:STRING, city:STRING, state:STRING, product:STRING, amount:FLOAT, dayofweek:INTEGER'

pipeline_args = [
    '--project=bigdatainpractice1',
    '--runner=DataflowRunner',
    '--region=us-central1',
    '--staging_location=gs://grizzly-bucket/temp/staging/',
    '--temp_location=gs://grizzly-bucket/temp',
    '--streaming'
]



import logging

logging.basicConfig(level=logging.INFO)  # Set the log level to INFO

def cleanse_data(element):
    # Assuming the input is a dictionary of {field_name: value}
    result = {}
    for field in ['tr_time_str', 'first_name', 'last_name', 'city', 'state', 'product']:
        if field in element:
            # Remove leading and trailing whitespace
            result[field] = element[field].strip()

    # Parse string to float for the 'amount' field
    if 'amount' in element:
        try:
            result['amount'] = float(element['amount'])
        except ValueError:
            result['amount'] = None
            logging.error(f"Failed to parse 'amount': {element['amount']}")  # Log error message

    # Convert tr_time_str to datetime and get the day of the week
    if 'tr_time_str' in element:
        try:
            date_time_obj = datetime.strptime(element['tr_time_str'], "%Y-%m-%d %H:%M:%S")
            result['dayofweek'] = date_time_obj.weekday()
            result['tr_time_str'] = date_time_obj  # updating the tr_time_str to be a datetime object
        except ValueError:
            result['dayofweek'] = None
            logging.error(f"Failed to parse 'tr_time_str': {element['tr_time_str']}")  # Log error message

    logging.info(f"Processed element: {result}")  # Log info message
    return result


with beam.Pipeline(argv=pipeline_args) as p:
    # Read the data from Pub/Sub
    (p | 'Read from PubSub' >> beam.io.ReadFromPubSub(topic="projects/bigdatainpractice1/topics/aekanun-transactions")
       # Apply the cleanse_data function to each element
       | 'Cleanse Data' >> beam.Map(cleanse_data)
       # Write the results to BigQuery
       | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
             table=table_spec,
             schema=schema,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )
