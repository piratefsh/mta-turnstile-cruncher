#!/usr/bin/python3

"""
Clean up MTA Turnstile data generated from scraper:
https://github.com/piratefsh/mta-turnstile-scraper
"""

import sqlite3
from util import trace
import datetime as dt
import sys

# globals
connection = None
cursor = None

COLUMN_HEADERS      = "CA,UNIT,SCP,STATION,LINENAME,DIVISION,DATETIME,TIME,DESC,CUM_ENTRIES,CUM_EXITS,ENTRIES,EXITS".split(',') 
COLUMN_DATATYPES    = "TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,DATETIME,TIME,TEXT,INTEGER,INTEGER,INTEGER,INTEGER".split(',')

# rename ENTRIES and EXITS columns to CUM_ENTRIES and CUM_EXITS
def add_columns():
    # get column headers and datatypes
    headers = cursor.execute('pragma table_info("entries")').fetchall()
    header_datatype = ",".join([name + ',' + datatype for _, name, datatype, _, _, _ in headers])
    
    # rename old table
    # check if table exists
    tables = cursor.execute('select name from sqlite_master where type="table"').fetchall()
    if ('entries',) in tables:
        cursor.execute('ALTER TABLE entries RENAME TO tmp_entries')
    
    # create new table with correct columns
    header_and_datatypes = "id INTEGER PRIMARY KEY AUTOINCREMENT,  " + ", ".join([COLUMN_HEADERS[i] + ' ' + COLUMN_DATATYPES[i] for i in range(len(COLUMN_HEADERS))])
    create_query = 'CREATE TABLE entries (%s)' % header_and_datatypes
    cursor.execute(create_query)

    # copy contents
    same_cols   = "CA,UNIT,SCP,STATION,LINENAME,DIVISION,DATETIME,TIME,DESC,"
    from_cols   = same_cols + "ENTRIES,EXITS"
    to_cols     = same_cols + "CUM_ENTRIES,CUM_EXITS"
    copy_query  = 'INSERT INTO entries(%s) SELECT %s FROM tmp_entries' % (to_cols, from_cols)
    
    cursor.execute(copy_query)

    # delete temp table
    cursor.execute('drop table tmp_entries')

    connection.commit()
    return 

# get all unique turnstiles, ordered by datetime
def get_turnstiles():
    get_u_turnstile_query = "select SCP, UNIT" + \
        " from entries group by SCP, UNIT order by UNIT"
    turnstiles = cursor.execute(get_u_turnstile_query).fetchall()
    return turnstiles

# for each unique turnstile, find previous entry
def per_turnstile(ts):
    scp, unit = ts
    get_rows_for_ts_query = "select * from entries" + \
        " where SCP=? AND UNIT=? order by DATETIME"
    rows = cursor.execute(get_rows_for_ts_query, ts).fetchall()

    return rows

def crunch_turnstile_rows(rows):
    diff = None 
    diff_entry = None 
    diff_exit = None 

    index_cum_entries = 10
    index_cum_exits = 11

    for i in range(len(rows)):
        row = rows[i]
        # if has previous, find the diff
        if i - 1 >= 0:
            prev = rows[i-1]
            diff_entry  = row[index_cum_entries] - prev[index_cum_entries]
            diff_exit   = row[index_cum_exits] - prev[index_cum_exits]
            update_entry_exit(row, diff_entry, diff_exit)
    connection.commit()

def update_entry_exit(row, entry, exit):
    entry = 0 if entry < 0 else entry
    exit = 0 if exit < 0 else exit 

    query = 'update entries set ENTRIES=?, EXITS=? where id=?'
    params = (entry, exit, row[0])
    res = cursor.execute(query, params)
    # trace(query, params, res.fetchall())

# Return True if that is previous to this and is for
# same unit and scp
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

def test():
    open_db('test/testsept.db')
    turnstiles = get_turnstiles()
    assert len(turnstiles) == 4576

    trace('tests pass')

def main():
    if len(sys.argv) != 2:
        print('Usage: python mta_cleaner <db_path>')
        return

    open_db(sys.argv[1])
    add_columns()

    turnstiles = get_turnstiles()
    
    count = 0
    for ts in turnstiles:
        rows = per_turnstile(ts)
        crunch_turnstile_rows(rows)
        count += 1 
        if count % 100 == 0:
            trace(dt.datetime.now(), count, 'of', len(turnstiles))

main()
