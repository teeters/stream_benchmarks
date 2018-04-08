from confluent_kafka import Consumer, KafkaError
import boto3
from timeit import default_timer as timer
from decimal import Decimal
import argparse
import numpy as np

#kafka setup
conf = {'bootstrap.servers':'localhost:9092', 'group.id':'group0', 
	'default.topic.config': {'auto.offset.reset':'smallest'}}
topic = 'producer_benchmark'
c=Consumer(conf)
c.subscribe([topic])

#dynamodb setup
db = boto3.resource('dynamodb')
dbclient = boto3.client('dynamodb')
#create the table iff it does not exist
tablename = 'benchmark_kafka'
table = None
try:
	table = db.create_table(
		TableName = tablename,
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
	table.meta.client.get_waiter('table_exists').wait(TableName=tablename)
except dbclient.exceptions.ResourceInUseException:
	table = db.Table(tablename)
if table == None:
	print 'Could not get or create table', tableName
	exit(1)


def consume_messages(n):
	'''Consume n messages from topic, or as many as possible.'''
	times = []
	i=0
	running = True
	with table.batch_writer(overwrite_by_pkeys=['creation_time','consumer_time' ]) as batch:
		while running:
			msg = c.poll()
			if not msg.error():
				#record producer time, consumer time, and database time
				producertime = float(msg.key())
				consumertime = timer()
				batch.put_item({
					'creation_time':Decimal(msg.key()), 
					'consumer_time':Decimal(consumertime+i),
					'value':msg.value()
				})
				databasetime = timer()
				times.append((producertime, consumertime, databasetime))
				#print databasetime-consumertime
				i+=1
			elif msg.error().code() == KafkaError._PARTITION_EOF:
				print 'Topic empty'
				running=False
			else:
				print "Error:", msg.error()
				running=False

			if i>=n:
				running=False
	return times

def main():
	'''Consume n messages and compute the consumption/database write time for each'''
	parser = argparse.ArgumentParser()
	parser.add_argument('n', help="Number of messages to consume", type=int)
	args = parser.parse_args()
	times = np.asarray(consume_messages(args.n))
	con_times = times[:,2] - times[:,1]
	print 'Times for', args.n, 'messages:'
	print 'Average:', np.mean(con_times)
	print '50th percentile:', np.percentile(con_times, 50)
	print '99th percentile:', np.percentile(con_times, 99)	

if __name__ == '__main__':
	main()
