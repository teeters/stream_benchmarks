from confluent_kafka import Producer
import numpy as np
import socket
import random
import time
from timeit import default_timer as timer
import string
import argparse
import os

topic_name = "producer_benchmark"
server_addr = 'localhost:9092'

def gen_records(num_records, record_size):
	'''Generate num_records records of record_size bytes each'''
	records = []
	for i in xrange(num_records):
		record = ''.join(random.choice(string.lowercase) for j in xrange(record_size))
		records.append(record)
	return records

def experiment(num_records, record_size, batch_size):
	'''Return statistics on transmission time for messages'''
	print 'Generating records'
	records = gen_records(num_records, record_size)
	conf = {'bootstrap.servers':server_addr,
		'client.id':socket.gethostname(),
		'batch.num.messages':int(batch_size/record_size),
		#'api.version.request':False,
		#'broker.version.fallback':'0.8.2.1'
		}
	
	print 'Sending records'
	producer = Producer(conf)
	times = []
	
	for r in records:
		try:
			start = timer()
			producer.produce(topic=topic_name, value=r, key=str(time.time()), 
				callback=lambda err, msg: record_interval(err, msg, start, timer(), times))
		except BufferError as e:
			print "Local queue is full,", len(producer), 'messages awaiting delivery.'
		producer.poll(0)
	#wait for all messages to be delivered
	producer.flush()
	return times

def record_interval(err, msg, start, end, times):
	if err:
		print str(err)
	else:
		times.append(end-start)


def get_batch_statistics(record_stats, messages_per_batch):
        '''Sum together record latency times to get the latency for each batch.'''
        sums = []
        for i in xrange(0, len(record_stats), messages_per_batch):
                sums.append(sum(record_stats[i:i+messages_per_batch])/messages_per_batch)
        return sums

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('-n', '--num_records', help='Number of messages (total) to produce', type=int, default=10000)
	parser.add_argument('-b', '--batch_size', help='Size of aggregated batch message (B)', type=int, default=1000000)
	parser.add_argument('-r', '--record_size', help='Size of each record (B)', type=int, required=True)
	args = parser.parse_args()
	batch_size = args.batch_size
	num_records = args.num_records
	record_size = args.record_size
	print 'Times for', num_records, 'records,', record_size, 'B each,', batch_size, 'B batch size.'
	record_times= experiment(num_records, record_size, batch_size)
	batch_times = get_batch_statistics(record_times, batch_size/record_size)
	batch_times = np.asarray(batch_times)
	print "Average time:", np.mean(batch_times)
        print '50th percentile:', np.percentile(batch_times, 50)
        print '99th percentile:', np.percentile(batch_times, 99)
	

if __name__ == '__main__':
	main() 
