from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys

class WordCounter(Bolt):

    def initialize(self, conf, ctx):
        self.counts = Counter()
	
        #Define our connection string
        conn_string = "host='localhost' dbname='tcount' user='postgres' password=''"
 
	# get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(conn_string)
 
	# conn.cursor will return a cursor object, you can use this cursor to perform queries
        cursor = conn.cursor()
 
 
    def process(self, tup):
        word = tup.values[0]

        # Write codes to increment the word count in Postgres
        # Use psycopg to interact with Postgres
        # Database name: Tcount 
        # Table name: Tweetwordcount 
        # you need to create both the database and the table in advance.
        

        # Increment the local count
        self.counts[word] += 1
        self.emit([word, self.counts[word]])

        # Log the count - just to see the topology running
        self.log('%s: %d' % (word, self.counts[word]))

                #Define our connection string
        conn_string = "host='localhost' dbname='tcount' user='postgres' password=''"

        # get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(conn_string)

        # conn.cursor will return a cursor object, you can use this cursor to perform queries
        cursor = conn.cursor()
        
        if(self.counts[word] == 1):
            cursor.execute("INSERT INTO tweetwordcount (word,count) VALUES (%s, 1);", (word,))
        else:
            cursor.execute("UPDATE tweetwordcount SET count=(count+1) WHERE word=%s", (word,))
        conn.commit()
        
        conn.close()
