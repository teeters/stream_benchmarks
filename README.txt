This file briefly describes the project and how to replicate the experiments conducted in the project.

kafka_prod_con.py: This is the main benchmarking program for Kafka. Run it with -h to view the options for
	setting the number of producers, consumers, batch size, message size, and number of messages.
	Note that changing the number of producers or consumers will not automatically reconfigure
	the Kafka server with the correct number of partitions--you will need to do this manually using
	the scripts in the kafka/bin directory

run_kafka_test.py: This is a driver script for kafka_prod_con; use it to run large batches of tests at once.
	The parameters for the tests are stored in a hard-coded array. Edit it to specify your own experiments.

run_tests.sh: This driver script runs the collection of python Kinesis programs necessary to run an end-to-end 
test using DynamoDB.

run_sql_tests.sh This driver script runs the collection of python Kinesis programs necessary to run an end-to-end 
test using MySQL.

delete_kafa_table.py
delete_kinesis_table.py These scripts delete the DynamoDB tables associated with Kafka and Kinesis. 
	This is the most efficient way to empty the tables if they are affecting performance.

clean_kafka.sh: This script is important. It flushes messages from the Kafka topic; use it if you have
	old messages in the stream throwing off timing measurements. Alternatively, you can run a consumer
	until it consumes all messages in the stream

kafka_consumer.py
kafka_producer.py: These scripts implement single-thread Kafka consumers and producers. They are outdated
	and you should use kafka_prod_con.py for timing instead, but they could be useful for filling/emptying the stream.

kin_producer_timings.py
kin_producer.py: These scripts upload data to a Kinesis stream.

kin_sql_consumer.py: This script consumes Kinesis data from an SQL table.

kin_db_consumer.py: This script consumes Kinesis data from a DynamoDB table.

kin_new.py: Removes any existing MySQL tables and replaces it with a new table for Kinesis.

kin_db_table_create.py: Removes existing DynamoDB table and replaces it with a new table for Kinesis.

sql_lambda: Is a Python deployment package for Lambda to transfer data from a Kinesis stream to a MySQL table.

kin_lambda: Is a Python deployment package for Lambda to transfer data from a Kinesis stream to a DynamoDB table.
