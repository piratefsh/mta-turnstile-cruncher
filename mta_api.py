
"""
Get for each station, entries and exits for each date in JSON format:
{
    stations: {
        'R170': {
            station_name: '14 ST - UNION SQ',
            turnstiles: 38,
            total_entries: 2675454,
            total_exits: 2405449, 
            dates: {
                '2015-09-09': {
                    times: [
                        {
                            time: '00:00:00',
                            entries: 312,
                            exits: 97
                        },
                        {
                            time: '04:00:00',
                            entries: 723,
                            exits: 49
                        }
                    ]
                },
                '2015-09-10': {
                    times: [
                        {
                            time: '00:00:00',
                            entries: 312,
                            exits: 97
                        },
                        {
                            time: '04:00:00',
                            entries: 723,
                            exits: 49
                        }
                    ]
                }
            }
        }
    }
}
"""

import sqlite3
import json

# get data from db
connection = sqlite3.connect(
    '../mta-turnstile-cruncher/db/mta-2015-09-clean.db')
cursor = connection.cursor()

def data_to_json(date_range=None):
    # get all stations
    stations = get_stations()

    stations_data = dict()
    # for each station, get total per time frame
    for unit, in stations:
        print(unit, end=', ')
        if not unit:
            print('bad unit', unit)
            continue
        station_name, num_turnstiles, entries, exits = get_metadata(unit)
        dates = get_numbers_by_date_time(unit, date_range)

        s = {
            'station_name': station_name,
            'turnstiles': num_turnstiles,
            'total_entries': entries,
            'total_exits': exits, 
            'dates' : dates
        }

        stations_data[unit] = s

    start, end = date_range
    data = {
        'stations' : stations_data,
        'date_range': {
            'start': start,
            'end' : end
        }
    }

    return data

def get_numbers_by_date_time(unit, dates=None):
        
    query = 'select DATETIME, sum(ENTRIES) as TOTAL_ENTRIES, sum(EXITS) as TOTAL_EXITS from entries where UNIT=? %s group by DATETIME order by DATETIME;'
    
    date_condition = ""
    if dates is not None:
        start, end = dates
        date_condition = 'and datetime(DATETIME) between datetime("%s") and datetime("%s")' % (start, end)
    query = query % (date_condition)
    res = cursor.execute(query , (unit,)).fetchall()
    dates = dict()

    for row in res:
        datetime, entries, exits = row 
        date, time = datetime.split(' ')
        
        t = {
            'time': time,
            'entries': entries,
            'exits' : exits
        }   

        if date in dates:
            dates[date]['times'].append(t)
        else:
            dates[date] = {
                'times': [t]
            }

    return dates 

def get_metadata(unit):
    query = "select STATION, count(distinct SCP), sum(ENTRIES), sum(EXITS) from entries where UNIT=?"
    res = cursor.execute(query, (unit,)).fetchone()
    return res


def get_stations():
    query = "select UNIT" + \
        " from entries group by UNIT order by UNIT"
    stations = cursor.execute(query).fetchall()
    return stations

def test(filename, dates):
    res = get_numbers_by_date_time('R170', ('2015-09-09', '2015-09-10')) 

    d = data_to_json(date_range=dates)
    
    with open(filename, 'w') as f:
        f.write(json.dumps(d, sort_keys=True, indent=2))
        f.close()


