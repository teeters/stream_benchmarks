import boto3
import aws_kinesis_agg.aggregator
from timeit import default_timer as timer
import time
import random
import string
import numpy as np
import argparse
from decimal import Decimal

stream_name='benchmark_stream'

num_records = 20000
record_size = 100 
agg_size = 1000000 
consume_size = 10000
table_name = 'benchmark_kinesis'
hash_name = 'dbhash'
client = boto3.client('kinesis', region_name='us-west-2')
db_client = boto3.client('dynamodb', region_name='us-west-2')
dbstream_client = boto3.client('dynamodbstreams', region_name='us-west-2')

def timed_put(record):
	'''Return time it takes to put record to kinesis'''
	start = timer()
	response = put(record)
	end = timer()
	return end-start

def put(record):
	client.put_record(StreamName=stream_name, 
                          Data=record, 
                          PartitionKey=str(time.time()))

def experiment(num_records, record_size, agg_size):	

	print "Retrieving records"
	total = 0
	times = []
	start = timer()
	response = db_client.scan(TableName=table_name, Select='ALL_ATTRIBUTES')
	end = timer()
	times.append(end-start)
	total += end-start
	#count = 0
	for item in response['Items']:
		#if count == 0:
		#	print "creation time:", float(item['creation_time']['N'])
		#	print "consumer time:", float(item['consumer_time']['N'])
		#	print "diff:", float(item['consumer_time']['N']) - float(item['creation_time']['N'])
	        #count = count + 1	
		times.append(float(item['consumer_time']['N'])-float(item['creation_time']['N']))
	print "Time per record:", total / response['Count']

	return times

def main():
        parser = argparse.ArgumentParser(description="Measure consumption time for some randomly generated Kinesis records")
        parser.add_argument('-n', '--num_records', help='Total number of records to generate', type=int, default=200000)
        parser.add_argument('-a', '--agg_size', help='Max size of aggregated kinesis record (B)', type=int, default=1000000)
        parser.add_argument('-r', '--record_size', help='Size of each record (B)', type=int, default=100)

        args = parser.parse_args()
        times = np.asarray(experiment(args.num_records, args.record_size, args.agg_size))
        print "Average time:", np.mean(times)
        print '50th percentile:', np.percentile(times, 50)
        print '99th percentile:', np.percentile(times, 99)

if __name__ == '__main__':
        main()
