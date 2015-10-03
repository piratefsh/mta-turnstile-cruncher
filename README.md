# MTA Turnstile Data Cleaner


## What this does
From a database generated from the [mta-turnstile-scraper](https://github.com/piratefsh/mta-turnstile-scraper), find the entries and exits for each row. 

Converts columns ENTRIES to CUM_ENTRIES and EXITS to CUM_EXITS. Updates calculated entries and exits in ENTRIES and EXITS columns respectively.

Raw data only has cumulative numbers per row, so some math and massaging was needed to get the exact numbers at each logged time slot. Does it by finding all entries per unique turnstile and subtracting the CUM_ENTRIES from a previous entry. Also ignores negative entries/exits and entries with no previous CUM_ENTRIES/CUM_EXITS.

## Usage

### Input

```
$ python mta_cleaner.py <path to db of data>
```

### Output

```
id|CA|UNIT|SCP|STATION|LINENAME|DIVISION|DATETIME|TIME|DESC|CUM_ENTRIES|CUM_EXITS|ENTRIES|EXITS
1|A002|R051|02-00-00|LEXINGTON AVE|NQR456|BMT|2015-09-19 00:00:00|00:00:00|REGULAR|5317608|1797091||
2|A002|R051|02-00-00|LEXINGTON AVE|NQR456|BMT|2015-09-19 04:00:00|04:00:00|REGULAR|5317644|1797096|36|5
3|A002|R051|02-00-00|LEXINGTON AVE|NQR456|BMT|2015-09-19 08:00:00|08:00:00|REGULAR|5317675|1797116|31|20
4|A002|R051|02-00-00|LEXINGTON AVE|NQR456|BMT|2015-09-19 12:00:00|12:00:00|REGULAR|5317778|1797215|103|99
5|A002|R051|02-00-00|LEXINGTON AVE|NQR456|BMT|2015-09-19 16:00:00|16:00:00|REGULAR|5318058|1797266|280|51
```


## Stuff I found out from the cleaned data

### Find total by turnstile

```
select ID,SCP, UNIT, STATION, ts.TOTAL_ENT as TOTAL_ENTRIES from (select *,SUM(ENTRIES) as TOTAL_ENT from entries group by SCP,UNIT) ts order by STATION;
```

### Find total for one station

```
> select ID,UNIT, STATION, ts.TOTAL_ENT as TOTAL_ENTRIES from (select *,SUM(ENTRIES) as TOTAL_ENT from entries WHERE STATION='14 ST-UNION SQ' group by UNIT) ts;

ID          UNIT        STATION         TOTAL_ENTRIES
----------  ----------  --------------  -------------
728605      R170        14 ST-UNION SQ  225106428089 
```

### Find total by station

```
# top 10 entries
> select ID,UNIT, STATION, ts.TOTAL_ENT as TOTAL_ENTRIES from (select *, SUM(ENTRIES) as TOTAL_ENT from entries group by UNIT) ts order by TOTAL_ENTRIES DESC limit 10;

ID          UNIT        STATION          TOTAL_ENTRIES
----------  ----------  ---------------  -------------
635141      R011        42 ST-PA BUS TE  1305321095061
586808      R080        57 ST-7 AVE      1288256345231
714660      R084        59 ST-COLUMBUS   739883225413 
713284      R033        42 ST-TIMES SQ   656792232455 
705651      R028        FULTON ST        563220526064 
680062      R453        23 ST-6 AVE      521178110228 
729663      R131        23 ST            517934378933 
726965      R044        BROOKLYN BRIDGE  509834714918 
773955      R110        FLATBUSH AVE     496945235519 
716443      R452        72 ST            494767882795 
```

### Stations on the G line, in order of decresing popularity

The '7' exclusion is there to filter out the weird 42nd st stop that apparently has 'G' in it.

```

> select UNIT, STATION,LINENAME,ts.TOTAL_ENT as TOTAL_ENTRIES,ts.TOTAL_EX as TOTAL_EXITS from (select *,SUM(EXITS) as TOTAL_EX,SUM(ENTRIES) as TOTAL_ENT from entries where instr(LINENAME, 'G') and not instr(LINENAME, '7')  group by UNIT) ts order by TOTAL_ENTRIES DESC limit 30;

UNIT   STATION     LINENAME  TOTAL_ENTRIES         TOTAL_EXITS         
-----  ----------  --------  --------------------  --------------------
R359   COURT SQ    EMG       42686776964           3097964946          
R256   NASSAU AV   G         30329584142           9830425266          
R269   BEDFORD/NO  G         28813348373           8496318383          
R204   CHURCH AVE  FG        28133085368           7156064502          
R258   4 AVE       DFGMNR    8800979414            6510770868          
R220   CARROLL ST  FG        7087445681            4230062938          
R268   METROPOLIT  GL        6618093970            4236728648          
R129   BERGEN ST   FG        6034371654            3762354812          
R217   HOYT/SCHER  ACG       5016216558            5133762835          
R288   7 AV-PARK   FG        4235522401            1337593203          
R317   CLINTON-WA  G         3303797746            716951611           
R286   MYRTLE-WIL  G         3230666594            2101013721          
R241   15 ST-PROS  FG        3159222729            1395058404          
R299   BROADWAY    G         2432778191            1802209267          
R287   CLASSON AV  G         2340248666            2287034710          
R239   GREENPOINT  G         2286162763            1402662188          
R318   FULTON ST   G         2056953955            2119567171          
R289   FT HAMILTO  FG        1725898227            590853872           
R270   SMITH-9 ST  FG        1704099310            1274924271          
R316   FLUSHING A  G         1180127765            1159380049          
R360   VAN ALSTON  G         919771530             1042047252
```

## Other Resources
* [MTA Facts and Figures](http://web.mta.info/nyct/facts/ffsubway.htm)