from kafka_prod_con import run_exp
import numpy as np

#args are num_records, record_size, batch_size, num_producers, num_consumers
num_producers = 1
num_consumers = 1
tests = [
	(40000, 50, 1048576)
]

for params in tests:
	num_records, record_size, batch_size = params
	producer_times, consumer_times = run_exp(num_records, record_size, batch_size, num_producers, num_consumers)
	producer_times = np.asarray(producer_times)
        consumer_times = np.asarray(consumer_times)
        p_latency = consumer_times[:,1]-consumer_times[:,0]
        ee_latency = consumer_times[:,2] - consumer_times[:,0] #time from production to recorded in database
        c_latency = consumer_times[:,2] - consumer_times[:,1] #time for consumer execution
        print 'End to end latency (s) for', num_records, 'records,', record_size, 'B per record,', batch_size, 'B batches'
        print num_producers, 'producers,', num_consumers, 'consumers'
        print '-'*80
        format_str = '{:<18} {:<18} {:<18} {:<18}'
        print format_str.format('Statistic', 'Mean', 'Median', '99th percentile')

        print format_str.format('Producer latency', np.mean(p_latency), np.percentile(p_latency, 50), np.percentile(p_latency, 99))
        print format_str.format('Consumer latency', np.mean(c_latency), np.percentile(c_latency, 50), np.percentile(c_latency, 99))
        print format_str.format('End-to-end latency', np.mean(ee_latency), np.percentile(ee_latency, 50), np.percentile(ee_latency, 99))
	
