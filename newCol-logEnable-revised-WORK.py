import apache_beam as beam
import logging
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO)

table_spec = 'bigdatainpractice1:aekanun_dataflow_sql_dataset.aekanun_dfsqltable_sales'
schema = 'tr_time_str:DATETIME, first_name:STRING, last_name:STRING, city:STRING, state:STRING, product:STRING, amount:FLOAT, dayofweek:INTEGER'

pipeline_args = [
    '--project=bigdatainpractice1',
    '--runner=DataflowRunner',
    '--region=us-east1',
    '--staging_location=gs://grizzly-bucket/temp/staging/',
    '--temp_location=gs://grizzly-bucket/temp',
    '--streaming'
]

def cleanse_data(element):
    from datetime import datetime
    element = element.decode('utf-8')
    element = json.loads(element)

    result = {}
    for field in ['tr_time_str', 'first_name', 'last_name', 'city', 'state', 'product']:
        if field in element:
            result[field] = element[field].strip()

    if 'amount' in element:
        try:
            result['amount'] = float(element['amount'])
        except ValueError:
            result['amount'] = None
            logging.error(f"Failed to parse 'amount': {element['amount']}")

    if 'tr_time_str' in element:
        try:
            date_time_obj = datetime.strptime(element['tr_time_str'], "%Y-%m-%d %H:%M:%S")
            result['dayofweek'] = date_time_obj.weekday()
            # Convert the datetime object back to a string
            result['tr_time_str'] = date_time_obj.strftime("%Y-%m-%d %H:%M:%S")  
        except ValueError:
            result['dayofweek'] = None
            logging.error(f"Failed to parse 'tr_time_str': {element['tr_time_str']}")

    logging.info(f"Processed element: {result}") 
    return result

with beam.Pipeline(argv=pipeline_args) as p:
    (p | 'Read from PubSub' >> beam.io.ReadFromPubSub(topic="projects/bigdatainpractice1/topics/aekanun-transactions")
       | 'Cleanse Data' >> beam.Map(cleanse_data)
       | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
             table=table_spec,
             schema=schema,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )

