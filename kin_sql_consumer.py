import boto3
import aws_kinesis_agg.aggregator
from timeit import default_timer as timer
import time
import random
import string
import numpy as np
import argparse
from decimal import Decimal
import MySQLdb

stream_name='benchmark_stream'

num_records = 20000
record_size = 100 
agg_size = 1000000 
consume_size = 10000

mysql_host = '54.212.197.235'
mysql_username = 'rts'
mysql_password = 'SamWangRamsay520-S'
mysql_dbname = 'rts_kinesis'
mysql_tablename = 'benchmark_kinesis'

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
	times = []
	db = MySQLdb.connect(host=mysql_host,
                                user=mysql_username,
                                passwd=mysql_password,
                                db=mysql_dbname )
	cursor = db.cursor()
	cursor.execute("SELECT * FROM benchmark_kinesis")
	numrows = cursor.rowcount
	#count = 0
	for row in range(0, numrows):	
		row = cursor.fetchone()
		#if count == 0:
		#	print "creation time:", float(item['creation_time']['N'])
		#	print "consumer time:", float(item['consumer_time']['N'])
		#	print "diff:", float(item['consumer_time']['N']) - float(item['creation_time']['N'])
	        #count = count + 1	
		times.append(float(row[1])-float(row[0]))

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
