#!/usr/bin/python3

"""
Clean up MTA Turnstile data generated from scraper:
https://github.com/piratefsh/mta-turnstile-scraper
"""

import sqlite3
from util import trace

# constants
COLUMN_HEADERS = "CA,UNIT,SCP,STATION,LINENAME,DIVISION,DATE,TIME,DESC,CUM_ENTRIES,CUM_EXITS,ENTRIES,EXITS".split(',')
COLUMN_DATATYPES = "TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,DATE,TIME,TEXT,INTEGER,INTEGER,INTEGER,INTEGER".split(',')
 
# globals
connection = None
cursor = None

# todo
# - for each entry, get entry and exists for that time period
def massage_db():
    # rename ENTRIES and EXITS row to CUM_ENTRIES and CUM_EXITS
    # get column headers and datatypes
    headers = cursor.execute('pragma table_info("entries")').fetchall()
    header_datatype = ",".join([name + ',' + datatype for _, name, datatype, _, _, _ in headers])
    
    # rename old table
    # check if table exists
    tables = cursor.execute('select name from sqlite_master where type="table"').fetchall()
    if ('entries',) in tables:
        if ('tmp_entries',) in tables:
            cursor.execute('DROP TABLE tmp_entries')
        cursor.execute('ALTER TABLE entries RENAME TO tmp_entries')
    
    # create new table with correct columns
    header_and_datatypes = "id INTEGER PRIMARY KEY AUTOINCREMENT,  " + ", ".join([COLUMN_HEADERS[i] + ' ' + COLUMN_DATATYPES[i] for i in range(len(COLUMN_HEADERS))])
    create_query = 'CREATE TABLE entries (%s)' % header_and_datatypes
    cursor.execute(create_query)

    # copy contents
    from_cols = "CA,UNIT,SCP,STATION,LINENAME,DIVISION,DATE,TIME,DESC,ENTRIES,EXITS"
    to_cols = "CA,UNIT,SCP,STATION,LINENAME,DIVISION,DATE,TIME,DESC,CUM_ENTRIES,CUM_EXITS"
    copy_query = 'INSERT INTO entries(%s) SELECT %s FROM tmp_entries' % (to_cols, from_cols)
    
    cursor.execute(copy_query)
    connection.commit()
    
    trace(copy_query)
    return


def open_db(dbname):
   global connection, cursor
   connection = sqlite3.connect(dbname)
   cursor = connection.cursor()
   return True

# - find stations with highest entry and exit traffic
# -      stations with largest difference
# -      line with highest and lowest ridership
