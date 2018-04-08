import boto3

#dynamodb setup
db = boto3.resource('dynamodb')
dbclient = boto3.client('dynamodb')
#create the table iff it does not exist
tablename = 'benchmark_kinesis'
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
