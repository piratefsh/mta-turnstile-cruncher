#!/usr/bin/python3

"""
Clean up MTA Turnstile data generated from scraper:
https://github.com/piratefsh/mta-turnstile-scraper
"""

import sqlite3
from util import trace
import datetime as dt
import sys

# constants
COLUMN_HEADERS = "CA,UNIT,SCP,STATION,LINENAME,DIVISION,DATETIME,TIME,DESC,CUM_ENTRIES,CUM_EXITS,ENTRIES,EXITS".split(',')
COLUMN_DATATYPES = "TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,DATETIME,TIME,TEXT,INTEGER,INTEGER,INTEGER,INTEGER".split(',')
 
# globals
connection = None
cursor = None

# todo
# - for each entry, get entry and exists for that time period
def add_rows():
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
    from_cols = "CA,UNIT,SCP,STATION,LINENAME,DIVISION,DATETIME,TIME,DESC,ENTRIES,EXITS"
    to_cols = "CA,UNIT,SCP,STATION,LINENAME,DIVISION,DATETIME,TIME,DESC,CUM_ENTRIES,CUM_EXITS"
    copy_query = 'INSERT INTO entries(%s) SELECT %s FROM tmp_entries' % (to_cols, from_cols)
    
    cursor.execute(copy_query)
    connection.commit()
    
    return 

def calc_entry_exits():
    select_query = 'select * from entries order by STATION, DATETIME ASC'
    max_unique_turnstiles_query = 'select max(y.num) from (select count(distinct SCP) as num from entries GROUP BY UNIT)y'
    max_unique_time_query = 'select max(y.num) from (select count(distinct TIME) as num from entries GROUP BY UNIT)y'

    unique_turnstiles = cursor.execute(max_unique_turnstiles_query).fetchone()[0]
    unique_time = cursor.execute(max_unique_time_query).fetchone()[0]
    #trace(unique_turnstiles, unique_time)
    max_offset = unique_turnstiles * unique_time

    rows = list(cursor.execute(select_query).fetchall())
    counter = 0
    while counter < len(rows):
        row = rows[counter]

        #trace(row)
        counter += 1 
        if(counter % 100 == 0):
            trace(counter, 'out of', len(rows))
            connection.commit()
        
        db_id, ca, unit, scp, station, line, _, date, time, _, cum_entries, cum_exits, _, _ = row
        res = get_prev_entry_by_timeslot(row, rows, start=counter-max_offset-1, end=counter-1)
     
        if res is None:
            continue

        prev_cum_entries, = res
        entries = cum_entries - prev_cum_entries 
        #trace(cum_entries, prev_cum_entries, entries)
        if entries < 0:
            continue

        update_query = 'UPDATE entries set ENTRIES=? WHERE id=?'
        cursor.execute(update_query, (entries, db_id))
        #trace(station, entries, db_id)
        
    connection.commit()

def get_prev_entry_by_timeslot(this, others, start=0, end=None):
    # ithis is index of this, or where to stop searching
    if end is None:
        end = len(others)
    if start < 0:
        start = 0
    # unroll data
    db_id, ca, unit, scp, station, line, _, date, time, _, cum_entries, cum_exits, _, _ = this 
    # if entry/exit is not 0
    if cum_entries < 0:
        return None 
    if cum_entries == 0:
        return (0,)

    prevs = [row for row in others[start:end] 
                if unit == row[2] and scp==row[3] 
                    and is_prev_entry(this, row)]
    
    # find previous time
    if len(prevs) > 0:
        prev = max(prevs, key=lambda x: x[7])
        return (prev[10],)
    return None # no previous time 

# return True if that is previous to this and is for same unit and scp
def is_prev_entry(this, that):
    idate = 7
    this_date = parsedate(this[idate])
    that_date = parsedate(that[idate])
    return that_date < this_date 

def parsedate(date):
    dateformat = '%Y-%m-%d %H:%M:%S' 
    return dt.datetime.strptime(date, dateformat)

def open_db(dbname):
   global connection, cursor
   connection = sqlite3.connect(dbname)
   cursor = connection.cursor()
   return True

# - find stations with highest entry and exit traffic
# -      stations with largest difference
# -      line with highest and lowest ridership

def test():
    # case: has previous of same day
    open_db('test/testsept.db')
    row = cursor.execute('SELECT * FROM entries WHERE id=?', (173178,)).fetchone()
    others = list(cursor.execute('SELECT * FROM entries').fetchall())
    prev = get_prev_entry_by_timeslot(row, others)
    assert prev[0] == 2080793776 
   
    # case: entries has reset to 0
    row_entries_0 = cursor.execute('SELECT * FROM entries WHERE id=?', (117285,)).fetchone()
    prev_0 = get_prev_entry_by_timeslot(row_entries_0, others)
    assert prev_0 == (0,) 
   
    #case: has previous, but on day before
    row_time_00 = cursor.execute('SELECT * FROM entries WHERE id=?', (7,)).fetchone()
    prev_time_00 = get_prev_entry_by_timeslot(row_time_00, others)
    assert prev_time_00 is not None 
    assert prev_time_00[0] == 5318468 

    # case: no previous time 
    row_time_00_no_prev = cursor.execute('SELECT * FROM entries WHERE id=?', (30385,)).fetchone()
    prev_time_00_none = get_prev_entry_by_timeslot(row_time_00_no_prev, others)
    assert prev_time_00_none  == None 
    
    lexave59 = cursor.execute('SELECT * FROM entries WHERE id=?', (2,)).fetchone()
    prev_lexave59 = get_prev_entry_by_timeslot(lexave59, others)
    assert prev_lexave59 is not None
    assert prev_lexave59[0] == 5317608 


    trace('test pass')

if len(sys.argv) == 2:
    dbname = sys.argv[1]
    open_db(dbname)
    calc_entry_exits()
