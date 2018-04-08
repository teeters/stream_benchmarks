from confluent_kafka import Producer, Consumer, KafkaError
import boto3
import numpy as np
import socket
import random
import time
from timeit import default_timer as timer
import string
import argparse
import os
from threading import Thread
from decimal import Decimal
import MySQLdb

topic_name = "roundtrip_benchmark"
server_addr = 'localhost:9092'

################################################################################
# DATABASES
################################################################################

#mysql setup
mysql_host = 'ec2-54-212-197-235.us-west-2.compute.amazonaws.com'
mysql_username = 'rts'
mysql_password = 'SamWangRamsay520-S'
mysql_dbname = 'rts_kafka'
mysql_tablename = 'benchmark_kafka'

_mysql_buffer = [] #ad-hoc message buffering for mysql, equivalent to dynamodb batch-write behavior
_mysql_buffer_limit = 25
def mysql_put(cursor, creation_time, consumer_time, value, times):
	'''Write the message to the mysql database'''
	global _mysql_buffer
	global _mysql_buffer_limit
	sql = 'INSERT INTO '+mysql_tablename+'(creation_time, consumer_time, value) VALUES (%s, %s, %s)'
	_mysql_buffer.append((creation_time, consumer_time, value))
	if len(_mysql_buffer) > _mysql_buffer_limit:
		cursor.executemany(sql, _mysql_buffer)
		_mysql_buffer = [] 

#dynamodb setup
db = boto3.resource('dynamodb')
dbclient = boto3.client('dynamodb')
#create the table iff it does not exist
dynamodb_tablename = 'benchmark_kafka'
dynamodb_table = None
try:
        dynamodb_table = db.create_table(
                TableName = dynamodb_tablename,
                KeySchema=[{'AttributeName':'creation_time', 'KeyType':'HASH'},
                        {'AttributeName':'consumer_time', 'KeyType':'RANGE'}],
                AttributeDefinitions=[
                        {'AttributeName':'creation_time','AttributeType':'N'},
                        {'AttributeName':'consumer_time','AttributeType':'N'}
                ],
                ProvisionedThroughput={
                        'ReadCapacityUnits':123,
                        'WriteCapacityUnits':123
                })
        table.meta.client.get_waiter('table_exists').wait(TableName=dynamodb_tablename)
except dbclient.exceptions.ResourceInUseException:
        dynamodb_table = db.Table(dynamodb_tablename)
if dynamodb_table == None:
        print 'Could not get or create table', dynamodb_tablename
        exit(1)

#write records to dynamodb batch
_dynamodb_buffered_times = []
def dynamodb_put(batch, creation_time, consumer_time, value, times):
	#since messages are not actually written to the database until the buffer
	#is written out, wait until then to record the final arrival time
	#for each message in the buffer
	#global _dynamodb_buffered_times
	#_dynamodb_buffered_times.append((creation_time, consumer_time))
	batch.put_item({
		'creation_time':Decimal(creation_time),
		'consumer_time':Decimal(consumer_time),
			'value':value
		})
	#if len(batch._items_buffer) == 0:
	#	db_time = timer()
	#	for (t1, t2) in _dynamodb_buffered_times:
	#		#times.append((t1,t2,db_time))
	#		pass
	#	_dynamodb_buffered_times = []

#database agnostic wrapper functions
def db_put(db_type, db_conn, creation_time, consumer_time, value, times):
	'''Write the message to the database specified by db_type (either 'dynamodb'
	or 'mysql') using the connection object db_conn.'''
	if db_type=='mysql':
		mysql_put(db_conn, creation_time, consumer_time, value, times)
	elif db_type=='dynamodb':
		dynamodb_put(db_conn, creation_time, consumer_time, value, times)
	else:
		print 'Error: unknown database type', db_type


def get_db_conn(db_type):
	if db_type=='mysql':
		conn = MySQLdb.connect( host=mysql_host, 
					user=mysql_username, 
					passwd=mysql_password, 
					db=mysql_dbname )
		return conn
	elif db_type=='dynamodb':
		return dynamodb_table.batch_writer(overwrite_by_pkeys=['creation_time', 'consumer_time']) 
	
####################################################################################
# CONSUMER SECTION
####################################################################################

#kafka setup
conf = {'bootstrap.servers':server_addr, 'group.id':'group0',
        'default.topic.config': {'auto.offset.reset':'smallest'}}
c=Consumer(conf)
c.subscribe([topic_name])

def consume_records(n, times, db_type='dynamodb'):
        '''Consume n messages from topic, or as many as possible.'''
        i=0
        running = True
        with get_db_conn(db_type) as db_conn:
                while running:
                        msg = c.poll()
                        if not msg.error():
                                #record producer time, consumer time, and database time
                                consumertime = timer()
				creationtime = float(msg.key())
				value = msg.value()
				db_put(db_type, db_conn, creationtime, consumertime, value, times)
				times.append((creationtime, consumertime, timer()))
				i+=1
                        elif msg.error().code() == KafkaError._PARTITION_EOF:
                                print 'Topic empty'
                        else:
                                print "Error:", msg.error()
                                running=False

                        if i>=n:
                                running=False
        return times

####################################################################################
# PRODUCER SECTION
####################################################################################


def gen_records(num_records, record_size):
        '''Generate num_records records of record_size bytes each'''
        records = []
        for i in xrange(num_records):
                record = ''.join(random.choice(string.lowercase) for j in xrange(record_size))
                records.append(record)
        return records

def produce_records(num_records, record_size, batch_size, times):
        '''Record statistics on transmission time for messages'''
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

####################################################################################
# DRIVER
####################################################################################

def run_exp(num_records, record_size, batch_size, num_producers=1, num_consumers=1, db_type='dynamodb'):
	'''Get producer and round-trip time statistics for the given workload'''
	#store data from threads in mutable lists
	#just don't access them till all tasks are complete
	producer_results = list([] for i in xrange(num_producers))
	consumer_results = list([] for i in xrange(num_consumers))
	threads = []
	
	#start consumers first
	for i in xrange(num_consumers):
		process = Thread(target=consume_records, args=[num_records/num_consumers, consumer_results[i], db_type])
		threads.append(process)		
		process.start()	

	for i in xrange(num_producers):
		process = Thread(target=produce_records,
			args=[num_records/num_producers, record_size, batch_size, producer_results[i]])
		threads.append(process)
		process.start()

	#wait for all threads to finish
	for process in threads:
		process.join()

	#aggregate data from all threads
	producer_times = []
	consumer_times = []
	for sublist in producer_results:
		for item in sublist:
			producer_times.append(item)

	for sublist in consumer_results:
		for item in sublist:
			consumer_times.append(item)
	#note: producer times will be list of producer latencies
	#whereas consumer times will be in format (time_produced, time_consumed, time_written_to_db)
	return (producer_times, consumer_times)

def main():
	parser = argparse.ArgumentParser()
        parser.add_argument('-n', '--num_records', help='Number of messages (total) to produce', type=int, default=10000)
        parser.add_argument('-b', '--batch_size', help='Size of aggregated batch message (B)', type=int, default=1000000)
        parser.add_argument('-r', '--record_size', help='Size of each record (B)', type=int, required=True)
	parser.add_argument('-p', '--num_producers', help='Number of producer threads', type=int, default=1)
	parser.add_argument('-c', '--num_consumers', help='Number of consumer threads. Note that Kafka\'s replication factor must be configured for this to have any effect!', type=int, default=1)
	parser.add_argument('-db', '--dbtype', help='Type of database to use. Choices are "mysql" and "dynamodb"', choices=set(('mysql', 'dynamodb')), default='dynamodb')
        args = parser.parse_args()
        batch_size = args.batch_size
        num_records = args.num_records
        record_size = args.record_size
	db_type = args.dbtype
	
	producer_times, consumer_times = run_exp(num_records, record_size, batch_size, args.num_producers, args.num_consumers, db_type)	
	producer_times = np.asarray(producer_times)
	consumer_times = np.asarray(consumer_times)
	
	p_latency = consumer_times[:,1]-consumer_times[:,0]
	ee_latency = consumer_times[:,2] - consumer_times[:,0] #time from production to recorded in database
	c_latency = consumer_times[:,2] - consumer_times[:,1] #time for consumer execution
	print 'End to end latency (s) for', num_records, 'records,', record_size, 'B per record,', batch_size, 'B batches'
	print args.num_producers, 'producers,', args.num_consumers, 'consumers', 'dbtype=', db_type
	print '-'*80
	format_str = '{:<18} {:<18} {:<18} {:<18}'
	print format_str.format('Statistic', 'Mean', 'Median', '99th percentile')
	
	print format_str.format('Producer latency', np.mean(p_latency), np.percentile(p_latency, 50), np.percentile(p_latency, 99))	
	print format_str.format('Consumer latency', np.mean(c_latency), np.percentile(c_latency, 50), np.percentile(c_latency, 99))
	print format_str.format('End-to-end latency', np.mean(ee_latency), np.percentile(ee_latency, 50), np.percentile(ee_latency, 99))


if __name__ == '__main__':
	main()
