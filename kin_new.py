import MySQLdb
import boto3

mysql_host = 'ec2-54-212-197-235.us-west-2.compute.amazonaws.com'
mysql_username = 'rts'
mysql_password = 'SamWangRamsay520-S'
mysql_dbname = 'rts_kinesis'
mysql_tablename = 'benchmark_kinesis'

conn = MySQLdb.connect(host=mysql_host,
                                        user=mysql_username,
                                        passwd=mysql_password,
                                        db=mysql_dbname )
cur = conn.cursor()

dropmysql = '''DROP TABLE IF EXISTS benchmark_kinesis'''

cur.execute(dropmysql)
print "Dropped table"

mysql = '''CREATE TABLE benchmark_kinesis (
       creation_time float(50) NOT NULL,
       consumer_time float(50) NOT NULL,
       value TEXT NOT NULL,
       PRIMARY KEY (creation_time, consumer_time)
       ) ENGINE=InnoDB DEFAULT CHARSET=latin1
       '''

sql = ("CREATE TABLE `benchmark_kinesis` ("
    "  `creation_time` varchar(100) NOT NULL,"
    "  `consumer_time` varchar(100) NOT NULL,"
    "  `value` varchar(100) NOT NULL,"
    "  PRIMARY KEY ('creation_time`)"
    ") ENGINE=InnoDB")

cur.execute(mysql)
print "Created table"
cur.close()
conn.close()
