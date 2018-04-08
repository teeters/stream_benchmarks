python kin_new.py 
sleep 1m
python kin_mysql_producer_timings.py -n 2500 -a 1048576 -r 800
sleep 5m
python kin_sql_consumer.py
