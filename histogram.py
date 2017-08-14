# histogram.py
#
# Program to count Tweetwordcount total word occurrences where counts are >= and <= an integer range passed as runtime parameters.
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys

#Command line arguments and runtime parameters
if len(sys.argv) != 2: 
    sys.exit("Usage: python histogram.py <lower range integer,upper range integer exmple: 3,8>")

# Parse argument string for integer range:
range = sys.argv[1].split(",")
if len(range) != 2:
   sys.exit("Usage: python histogram.py <lower range integer,upper range integer exmple: 3,8>")

#Define our connection string
conn_string = "host='localhost' dbname='tcount' user='postgres' password=''"

# print the connection string we will use to connect
print "Connecting to database\n	->%s" % (conn_string)

# get a connection, if a connect cannot be made an exception will be raised here
conn = psycopg2.connect(conn_string)

# conn.cursor will return a cursor object, you can use this cursor to perform queries
cursor = conn.cursor()
print "Connected!\n"

# Select from tweetcount table
cursor = conn.cursor()
cursor.execute("SELECT word, count FROM tweetwordcount WHERE count >= %s and count <= %s order by count desc;", (int(range[0]),int(range[1],)))
results = cursor.fetchall()
for rec in results:
   print "\"%s\": %d" % (rec[0], rec[1])
conn.commit()

conn.close()
