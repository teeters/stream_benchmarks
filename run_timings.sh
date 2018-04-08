python delete_kinesis_table.py 
sleep 2m
python kin_db_table_create.py
sleep 1m
python kin_producer_timings.py -n 1250 -a 1048576 -r 1600
sleep 5m
python kin_db_consumer.py
