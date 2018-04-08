import base64
import json
import aws_kinesis_agg
import boto3
import datetime
import time
from decimal import Decimal
#from MySQLdb import MySQLdb.connect
import mysql.connector

from aws_kinesis_agg.deaggregator import deaggregate_records

def lambda_handler(event, context):
    """
    Receive a batch of events from Kinesis and insert into our DynamoDB table
    """
    print('Received request')
    item = None

    mysql_host = '54.212.197.235'
    mysql_username = 'rts'
    mysql_password = 'SamWangRamsay520-S'
    mysql_dbname = 'rts_kinesis'
    mysql_tablename = 'benchmark_kinesis'

    print('Start connection')
    conn = mysql.connector.connect(host=mysql_host,
                                        user=mysql_username,
                                        passwd=mysql_password,
                                        db=mysql_dbname )
    print('End connection')
    '''Write the message to the mysql database'''
    cur = conn.cursor()

    #dynamo_db = boto3.resource('dynamodb')
    #table = dynamo_db.Table('benchmark_kinesis')
    _mysql_buffer = [] #ad-hoc message buffering for mysql, equivalent to dynamodb batch-write behavior
    _mysql_buffer_limit = 25
    records = [record for record in event['Records']]
    new_records = deaggregate_records(records)
    #decoded_record_data = [record['kinesis']['data'] for record in new_records]
    #deserialized_data = [decoded_record for decoded_record in records]
    #for data in decoded_record_data:
    for record in new_records:
	#d_record = "%.15g" % record['kinesis']['partitionKey']
	#con_time = "%.15g" % time.time()
	creation_time = Decimal(record['kinesis']['partitionKey'])
	consumer_time =  Decimal(time.time())
	value = record['kinesis']['data']
	#cur.execute('INSERT INTO '+mysql_tablename+'(creation_time, consumer_time, value) VALUES (%s, %s, %s)', (creation_time, consumer_time, value))
        sql = 'INSERT INTO '+mysql_tablename+'(creation_time, consumer_time, value) VALUES (%s, %s, %s)'
        _mysql_buffer.append((creation_time, consumer_time, value))
        if len(_mysql_buffer) > _mysql_buffer_limit:
                cur.executemany(sql, _mysql_buffer)
                _mysql_buffer = []
	# Add a processed time so we have a rough idea how far behind we are
        #item['processed'] = datetime.datetime.utcnow().isoformat()

    conn.commit()
    conn.close()
    cur.close()
    # Print the last item to make it easy to see how we're doing
    #print(json.dumps(item))
    print('Number of records: {}'.format(str(len(new_records))))
