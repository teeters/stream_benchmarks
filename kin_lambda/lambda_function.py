import base64
import json
import aws_kinesis_agg
import boto3
import datetime
import time
from decimal import Decimal

from aws_kinesis_agg.deaggregator import deaggregate_records

def lambda_handler(event, context):
    """
    Receive a batch of events from Kinesis and insert into our DynamoDB table
    """
    #print "time:", time.time()
    #print('Received request')
    item = None
    dynamo_db = boto3.resource('dynamodb')
    table = dynamo_db.Table('benchmark_kinesis')
    records = [record for record in event['Records']]
    new_records = deaggregate_records(records)
    #decoded_record_data = [record['kinesis']['data'] for record in new_records]
    #deserialized_data = [decoded_record for decoded_record in records]
    #for data in decoded_record_data:
    with table.batch_writer() as batch_writer:
        for record in new_records:
	    #d_record = "%.15g" % record['kinesis']['partitionKey']
	    #con_time = "%.15g" % time.time()
	    item = {
		'creation_time': Decimal(record['kinesis']['partitionKey']),
		'consumer_time': Decimal(time.time()),
                'value': record['kinesis']['data']
            }
            # Add a processed time so we have a rough idea how far behind we are
            #item['processed'] = datetime.datetime.utcnow().isoformat()
            batch_writer.put_item(Item=item)

    # Print the last item to make it easy to see how we're doing
    #print(json.dumps(item))
    #print "end time:", time.time()
    print('Number of records: {}'.format(str(len(new_records))))
