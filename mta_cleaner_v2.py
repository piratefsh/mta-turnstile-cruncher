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
        print('Usage: python mta_cleaner_v2 <db_path>')
        return

    open_db(sys.argv[1])
    turnstiles = get_turnstiles()
    
    count = 0
    for ts in turnstiles:
        rows = per_turnstile(ts)
        crunch_turnstile_rows(rows)
        count += 1 
        if count % 100 == 0:
            trace(dt.datetime.now(), count, 'of', len(turnstiles))

main()
