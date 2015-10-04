#!/usr/bin/python3

"""
Clean up MTA Turnstile data generated from scraper:
https://github.com/piratefsh/mta-turnstile-scraper
"""

import sqlite3
from util import trace
import datetime as dt
import sys
import numpy as np

# globals
connection = None
cursor = None
index_cum_entries = 10
index_cum_exits = 11
index_entries = 12
index_exits = 13

COLUMN_HEADERS = "CA,UNIT,SCP,STATION,LINENAME,DIVISION,DATETIME,TIME,DESC,CUM_ENTRIES,CUM_EXITS,ENTRIES,EXITS".split(
    ',')
COLUMN_DATATYPES = "TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,DATETIME,TIME,TEXT,INTEGER,INTEGER,INTEGER,INTEGER".split(
    ',')

# rename ENTRIES and EXITS columns to CUM_ENTRIES and CUM_EXITS


def add_columns():
    # get column headers and datatypes
    headers = cursor.execute('pragma table_info("entries")').fetchall()
    header_datatype = ",".join(
        [name + ',' + datatype for _, name, datatype, _, _, _ in headers])

    # rename old table
    # check if table exists
    tables = cursor.execute(
        'select name from sqlite_master where type="table"').fetchall()
    if ('entries',) in tables:
        cursor.execute('ALTER TABLE entries RENAME TO tmp_entries')

    # create new table with correct columns
    header_and_datatypes = "id INTEGER PRIMARY KEY AUTOINCREMENT,  " + \
        ", ".join([COLUMN_HEADERS[i] + ' ' + COLUMN_DATATYPES[i]
                   for i in range(len(COLUMN_HEADERS))])
    create_query = 'CREATE TABLE entries (%s)' % header_and_datatypes
    cursor.execute(create_query)

    # copy contents
    same_cols = "CA,UNIT,SCP,STATION,LINENAME,DIVISION,DATETIME,TIME,DESC,"
    from_cols = same_cols + "ENTRIES,EXITS"
    to_cols = same_cols + "CUM_ENTRIES,CUM_EXITS"
    copy_query = 'INSERT INTO entries(%s) SELECT %s FROM tmp_entries' % (
        to_cols, from_cols)

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

def get_stations():
    get_u_station_query = "select UNIT" + \
        " from entries group by UNIT order by UNIT"
    stations = cursor.execute(get_u_station_query).fetchall()
    return stations

def per_turnstile(ts):
    get_rows_for_ts_query = "select * from entries" + \
        " where SCP=? AND UNIT=? order by DATETIME"
    rows = cursor.execute(get_rows_for_ts_query, ts).fetchall()
    return rows

def per_station(st):
    get_rows_for_ts_query = "select * from entries" + \
        " where UNIT=? order by DATETIME"
    rows = cursor.execute(get_rows_for_ts_query, st).fetchall()
    return rows

def crunch_turnstile_rows(rows):
    global index_cum_entries, index_cum_exits

    diff = None
    diff_entry = None
    diff_exit = None

    for i in range(len(rows)):
        row = rows[i]

        row_centries = row[index_cum_entries]
        row_cexits = row[index_cum_exits]

        # if has previous, find the diff
        if i - 1 >= 0:
            prev = rows[i-1]
            diff_entry = row_centries - prev[index_cum_entries]
            diff_exit = row_cexits - prev[index_cum_exits]
            update_entry_exit(row, diff_entry, diff_exit)
    connection.commit()


def remove_outliers(rows):
    global index_entries, index_exits
    outliers = []

    for row in rows:
        en = row[index_entries] if row[index_entries] is not None else 0
        ex = row[index_exits] if row[index_exits] is not None else 0

        # find mean and std deviation
        mean_centries, std_centries = get_mean_and_std(rows, index_entries)
        mean_cexits, std_cexits = get_mean_and_std(rows, index_exits)

        # is this an outlier?
        outlier_entry = is_outlier(mean_centries, std_centries, en)
        outlier_exits = is_outlier(mean_cexits, std_cexits, ex)

        # discard outliers
        if outlier_entry or outlier_exits:
            outliers.append(row)
            if outlier_entry:
                en = None

            if outlier_exits:
                ex = None
            update_entry_exit(row, en, ex)
    connection.commit()
    return outliers


def is_outlier(mean, std, val):
    magnitude = 5
    return abs(mean - val) > std*magnitude


def get_mean_and_std(rows, i):
    data = [r[i] if r[i] is not None else 0 for r in rows]
    return (np.mean(data), np.std(data))


def update_entry_exit(row, entry, exit):
    entry = 0 if entry is not None and entry < 0 else entry
    exit = 0 if exit is not None and exit < 0 else exit

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
    return connection


def test():
    open_db('db/mta-2015-09-12.db')
    turnstiles = get_turnstiles()
    assert len(turnstiles) == 4579

    stations = get_stations()
    assert len(stations) == 465

    rows = per_station(stations[0])
    assert len(rows) > 0
    trace('tests pass')


def test_std_dev():
    data = """[(69660,"N203","R195","00-00-03","161 ST-YANKEE","BD4","IND","2015-09-09 08:22:00","08:22:00","REGULAR",2177371,742200,0,115),
(69661,"N203","R195","00-00-03","161 ST-YANKEE","BD4","IND","2015-09-09 12:22:00","12:22:00","REGULAR",2177371,742420,0,220),
(69662,"N203","R195","00-00-03","161 ST-YANKEE","BD4","IND","2015-09-09 16:22:00","16:22:00","REGULAR",2177371,742781,0,361),
(69663,"N203","R195","00-00-03","161 ST-YANKEE","BD4","IND","2015-09-09 20:22:00","20:22:00","REGULAR",2177371,743341,0,560),
(69664,"N203","R195","00-00-03","161 ST-YANKEE","BD4","IND","2015-09-10 00:22:00","00:22:00","REGULAR",2177371,743469,0,128),
(69665,"N203","R195","00-00-03","161 ST-YANKEE","BD4","IND","2015-09-10 04:22:00","04:22:00","REGULAR",2177371,743510,0,41),
(69666,"N203","R195","00-00-03","161 ST-YANKEE","BD4","IND","2015-09-10 08:22:00","08:22:00","REGULAR",553671722,184576396,551494351,183832886),
(69667,"N203","R195","00-00-03","161 ST-YANKEE","BD4","IND","2015-09-10 12:22:00","12:22:00","REGULAR",553672337,184576516,615,120),
(69668,"N203","R195","00-00-03","161 ST-YANKEE","BD4","IND","2015-09-10 16:22:00","16:22:00","REGULAR",553673019,184576738,682,222),
(69669,"N203","R195","00-00-03","161 ST-YANKEE","BD4","IND","2015-09-10 20:22:00","20:22:00","REGULAR",553673853,184577021,834,283),
(69670,"N203","R195","00-00-03","161 ST-YANKEE","BD4","IND","2015-09-11 00:22:00","00:22:00","REGULAR",553674073,184577133,220,112),
(69671,"N203","R195","00-00-03","161 ST-YANKEE","BD4","IND","2015-09-11 04:22:00","04:22:00","REGULAR",553674104,184577158,31,25),
(69672,"N203","R195","00-00-03","161 ST-YANKEE","BD4","IND","2015-09-11 08:22:00","08:22:00","REGULAR",553674630,184577234,526,76),
(69673,"N203","R195","00-00-03","161 ST-YANKEE","BD4","IND","2015-09-11 12:22:00","12:22:00","REGULAR",553675325,184577382,695,148),
(69674,"N203","R195","00-00-03","161 ST-YANKEE","BD4","IND","2015-09-11 16:22:00","16:22:00","REGULAR",553676083,184577614,758,232),
(69675,"N203","R195","00-00-03","161 ST-YANKEE","BD4","IND","2015-09-11 20:22:00","20:22:00","REGULAR",553676556,184578274,473,660)]"""

    dataset = eval(data)

    removed = remove_outliers(dataset)
    assert removed == 1
    trace('tests pass')

def remove_outliers_by(things, per_thing):
    trace('Removing outliers...')
    count = 0
    count_thing = 0
    for ts in things:
        rows = per_thing(ts)
        removed = remove_outliers(rows)

        if len(removed) > 0:
            one = removed[0]
            trace('Removed %d outliers from %s, station %s, id %d, %d %d' % (len(removed), str(ts), one[4], one[0], int(one[index_entries] or 0), int(one[index_exits] or 0)))
            
            count += 1
        count_thing += 1

        if(count_thing % 100 == 0):
            trace(dt.datetime.now(), 'Thing %d of %d' % (count_thing, len(things)))

    trace(dt.datetime.now(), 'Done. Removed %d points' % count)
    connection.commit()

def main():
    numargs = len(sys.argv) 
    if numargs < 2 or numargs > 3 :
        print('Usage: python mta_cleaner <db_path> <optional: clean>')
        return

    open_db(sys.argv[1])
 
    if numargs == 2:
        add_columns()

        turnstiles = get_turnstiles()

        count = 0
        for ts in turnstiles:
            rows = per_turnstile(ts)
            crunch_turnstile_rows(rows)
            count += 1
            if count % 100 == 0:
                trace(dt.datetime.now(), count, 'of', len(turnstiles))

    if numargs == 3 and sys.argv[2] == 'clean':
        turnstiles = get_turnstiles()
        remove_outliers_by(turnstiles, per_turnstile)
        # stations = get_stations()
        # remove_outliers_by(stations, per_station)
        
main()
