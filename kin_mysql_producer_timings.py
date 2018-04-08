import boto3
import aws_kinesis_agg.aggregator
import aws_kinesis_agg
from timeit import default_timer as timer
import time
import random
import string
import numpy as np
import argparse
import datetime

from aws_kinesis_agg.deaggregator import deaggregate_records

stream_name='mysql_stream'

num_records = 100
record_size = 400 
#agg_size = 1048576 
agg_size = num_records*record_size

client = boto3.client('kinesis', region_name='us-west-2')

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
	#generate records
	print "Generating records"
	records = [{'Data':''.join([random.choice(string.lowercase) for i in xrange(record_size)]),
			'PartitionKey':time.time()} for j in xrange(num_records)]
	#datetime.datetime.utcnow().isoformat()
	print "Putting records"

	kinesis_agg = aws_kinesis_agg.aggregator.RecordAggregator()
	times = []

	start = timer()
	for record in records:
		agg_finished=kinesis_agg.add_user_record(record['PartitionKey'], record['Data'], None)
		if agg_finished or kinesis_agg.get_size_bytes() >= agg_size:
			if not agg_finished: agg_finished = kinesis_agg.clear_and_get()
			pk, ehk, data = agg_finished.get_contents()
			client.put_record(StreamName=stream_name, Data=data, PartitionKey=pk, ExplicitHashKey=ehk)
			end = timer()
			times.append(end-start)
			time.sleep(1)
			start = timer()
	return times

def main():
	parser = argparse.ArgumentParser(description="Measure digestion time for some randomly generated Kinesis records")
	parser.add_argument('-n', '--num_records', help='Total number of records to generate', type=int, default=10000)
	parser.add_argument('-a', '--agg_size', help='Max size of aggregated kinesis record (B)', type=int, default=1043582)
	parser.add_argument('-r', '--record_size', help='Size of each record (B)', type=int, default=100)

	args = parser.parse_args()
	times = np.asarray(experiment(args.num_records, args.record_size, args.agg_size))
	print "Average time:", np.mean(times)
	print '50th percentile:', np.percentile(times, 50)
	print '99th percentile:', np.percentile(times, 99)

if __name__ == '__main__':
	main()
