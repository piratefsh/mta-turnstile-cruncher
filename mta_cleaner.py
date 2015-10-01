#!/usr/bin/python3

"""
Clean up MTA Turnstile data generated from scraper:
https://github.com/piratefsh/mta-turnstile-scraper
"""

import sqlite3
from util import trace
import datetime as dt

# constants
COLUMN_HEADERS = "CA,UNIT,SCP,STATION,LINENAME,DIVISION,DATE,TIME,DESC,CUM_ENTRIES,CUM_EXITS,ENTRIES,EXITS".split(',')
COLUMN_DATATYPES = "TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,DATE,TIME,TEXT,INTEGER,INTEGER,INTEGER,INTEGER".split(',')
 
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
    from_cols = "CA,UNIT,SCP,STATION,LINENAME,DIVISION,DATE,TIME,DESC,ENTRIES,EXITS"
    to_cols = "CA,UNIT,SCP,STATION,LINENAME,DIVISION,DATE,TIME,DESC,CUM_ENTRIES,CUM_EXITS"
    copy_query = 'INSERT INTO entries(%s) SELECT %s FROM tmp_entries' % (to_cols, from_cols)
    
    cursor.execute(copy_query)
    connection.commit()
    
    return 

def calc_entry_exits():
    rows = cursor.execute('SELECT * FROM entries')
    for row in rows:
       prev_cum_entries, = get_prev_entry_by_timeslot(row)

def get_prev_entry_by_timeslot(row):
    # unroll data
    # trace(row)
    db_id, ca, unit, scp, station, line, _, date, time, _, cum_entries, cum_exits, _, _ = row
    # if entry/exit is not 0
    trace(row)
    if cum_entries <= 0:
        return None


    # if there is an entry previous that matches
    select = "UNIT,SCP,DATE,TIME,CUM_ENTRIES FROM entries"
    prev = cursor.execute('SELECT %s WHERE id=? AND UNIT=? AND SCP=? AND DATE=?' % select, (str(db_id-1), unit, scp, date)).fetchone()
    if prev:
        # then find difference and log as entry/exit
        punit, pscp, pdate, ptime, pcum_entries = prev
        return (pcum_entries,) 
        
    else:
        # find last time of previous day and compare to that 
        dateformat = '%m/%d/%Y'
        yesterday = dt.datetime.strftime(dt.datetime.strptime(date, dateformat) - dt.timedelta(1), dateformat)
        prevs = cursor.execute('SELECT %s WHERE UNIT=? AND SCP=? AND DATE=? ORDER BY TIME' % select, (unit, scp, yesterday)).fetchall()
        if len(prevs) > 0:
            prev = prevs[-1]
            punit, pscp, pdate, ptime, pcum_entries = prev
            trace(prev)
            return (pcum_entries,)
    return None

def open_db(dbname):
   global connection, cursor
   connection = sqlite3.connect(dbname)
   cursor = connection.cursor()
   return True

# - find stations with highest entry and exit traffic
# -      stations with largest difference
# -      line with highest and lowest ridership

def test():
    open_db('test/mta-turnstile-2015-sept.db')
    row = cursor.execute('SELECT * FROM entries WHERE id=?', (173178,)).fetchone()
    prev = get_prev_entry_by_timeslot(row)
    # trace(row, prev)
    assert prev[0] == 2080793776 
    
    #id          CA          UNIT        SCP         STATION     LINENAME    DIVISION    DATE        TIME        DESC        CUM_ENTRIES  CUM_EXITS   ENTRIES     EXITS     
    #----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  -----------  ----------  ----------  ----------
    # 740286      R260        R205        01-05-01    149 ST-GR   245         IRT         09/04/2015  20:00:00    REGULAR     0            201 
    row_entries_0 = cursor.execute('SELECT * FROM entries WHERE id=?', (740286,)).fetchone()
    prev_0 = get_prev_entry_by_timeslot(row_entries_0)
    assert prev_0 == None
   
    #id          CA          UNIT        SCP         STATION     LINENAME    DIVISION    DATE        TIME        DESC        CUM_ENTRIES  CUM_EXITS   ENTRIES     EXITS     
    #----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  -----------  ----------  ----------  ----------
    #623536      J034        R007        00-00-03    104 ST      JZ          BMT         09/03/2015  20:00:00    REGULAR     3917798      4409278                           
    #623537      J034        R007        00-00-03    104 ST      JZ          BMT         09/04/2015  00:00:00    REGULAR     3917827      4409360        
    row_time_00 = cursor.execute('SELECT * FROM entries WHERE id=?', (623537,)).fetchone()
    prev_time_00 = get_prev_entry_by_timeslot(row_time_00)
    assert prev_time_00 is not None 
    assert prev_time_00[0] == 3917798

    #774712      S101        R070        00-00-00    ST. GEORGE     1           SRT         08/29/2015  00:00:00    REGULAR     744553       137  
    row_time_00_no_prev = cursor.execute('SELECT * FROM entries WHERE id=?', (774712,)).fetchone()
    prev_time_00_none = get_prev_entry_by_timeslot(row_time_00_no_prev)
    assert prev_time_00_none  == None

    trace('test pass')
