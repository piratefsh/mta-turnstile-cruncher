# MTA Turnstile Data Cleaner


## What this does
From a database generated from the [mta-turnstile-scraper](https://github.com/piratefsh/mta-turnstile-scraper), find the entries and exits for each row. 

Converts columns `ENTRIES` to `CUM_ENTRIES` and `EXITS` to `CUM_EXITS`. Updates calculated entries and exits in `ENTRIES` and `EXITS` columns respectively.

Raw data only has cumulative numbers per row, so some math and massaging was needed to get the exact numbers at each logged time slot. Does it by finding all entries per unique turnstile and subtracting the `CUM_ENTRIES` from a previous entry. Also ignores negative entries/exits and entries with no previous `CUM_ENTRIES/CUM_EXITS`.

## Usage

### Input

```
# first run
$ python mta_cleaner.py <path to db of data>

# then run to remove outliers only
$ python mta_cleaner.py <path to db of data> clean 
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

This is pulled from Sept 2015 data only, so just a relatively small dataset.


###Top 10 most popular stations for a week in Sept 2015

Query finds total by station and sorts by `TOTAL_ENTRIES`

```
> select ID, UNIT, STATION, LINENAME, ts.TOTAL_ENT as TOTAL_ENTRIES from (select *, SUM(ENTRIES) as TOTAL_ENT from entries group by UNIT) ts order by TOTAL_ENTRIES DESC limit 10;

ID          UNIT        STATION       LINENAME    TOTAL_ENTRIES
----------  ----------  ------------  ----------  -------------
728605      R170        14 ST-UNION   456LNQR     2675454      
733260      R046        42 ST-GRD CN  4567S       1789939      
679390      R022        34 ST-HERALD  BDFMNQR     1732995      
714660      R084        59 ST-COLUMB  1ABCD       1729229      
713284      R033        42 ST-TIMES   1237ACENQR  1584018      
637233      R012        34 ST-PENN S  ACE         1563049      
738398      R179        86 ST         456         1562741      
765251      R055        MAIN ST       7           1525426      
711512      R293        34 ST-PENN S  123ACE      1356457      
635141      R011        42 ST-PA BUS  ACENQRS123  1342426 
```

### Top 10 Stations on the 1 line

> select UNIT, STATION,LINENAME,ts.TOTAL_ENT as TOTAL_ENTRIES,ts.TOTAL_EX as TOTAL_EXITS from (select *,SUM(EXITS) as TOTAL_EX,SUM(ENTRIES) as TOTAL_ENT from entries where instr(LINENAME, '1') group by UNIT) ts order by TOTAL_ENTRIES DESC limit 10;

42nd st wins by a huge margin if you group them all up. Not sure how different 42 ST- PA BUS is from 42 ST- TIMES SQ. Considering consolidating them.

```
UNIT        STATION         LINENAME    TOTAL_ENTRIES  TOTAL_EXITS
----------  --------------  ----------  -------------  -----------
R084        59 ST-COLUMBUS  1ABCD       1729229        1362474    
R033        42 ST-TIMES SQ  1237ACENQR  1584018        1054274    
R293        34 ST-PENN STA  123ACE      1356457        923180     
R011        42 ST-PA BUS T  ACENQRS123  1342426        1116910    
R540        PATH WTC 2      1           1105583        1102541    
R168        96 ST           123         975422         917591     
R452        72 ST           123         975304         834867     
R010        42 ST-PA BUS T  ACENQRS123  931452         735392     
R032        42 ST-TIMES SQ  1237ACENQR  878550         1219331    
R541        THIRTY THIRD S  1           790941         719161     
R031        34 ST-PENN STA  123         711135         794023     
R001        SOUTH FERRY     1R          691317         664513  
```

### Stations on the G line, in order of decresing popularity

The '7' exclusion is there to filter out the weird 42nd st stop that apparently has 'G' in it.

```

> select UNIT, STATION,LINENAME,ts.TOTAL_ENT as TOTAL_ENTRIES,ts.TOTAL_EX as TOTAL_EXITS from (select *,SUM(EXITS) as TOTAL_EX,SUM(ENTRIES) as TOTAL_ENT from entries where instr(LINENAME, 'G') and not instr(LINENAME, '7')  group by UNIT) ts order by TOTAL_ENTRIES DESC limit 30;

UNIT        STATION          LINENAME    TOTAL_ENTRIES  TOTAL_EXITS
----------  ---------------  ----------  -------------  -----------
R268        METROPOLITAN AV  GL          406604         204217     
R359        COURT SQ         EMG         393372         206356     
R258        4 AVE            DFGMNR      317127         285127     
R288        7 AV-PARK SLOPE  FG          272005         60317      
R204        CHURCH AVE       FG          270687         169046     
R129        BERGEN ST        FG          270588         166326     
R220        CARROLL ST       FG          268265         121279     
R217        HOYT/SCHERMER    ACG         240597         213857     
R256        NASSAU AV        G           218548         113352     
R239        GREENPOINT AVE   G           218107         155785     
R269        BEDFORD/NOSTRAN  G           198039         110727     
R241        15 ST-PROSPECT   FG          146397         63455      
R286        MYRTLE-WILLOUGH  G           144565         79652      
R289        FT HAMILTON PKY  FG          134171         38559      
R317        CLINTON-WASH AV  G           132916         19985      
R270        SMITH-9 ST       FG          126743         142212     
R287        CLASSON AVE      G           122914         116782     
R318        FULTON ST        G           114912         110825     
R299        BROADWAY         G           102202         85013      
R316        FLUSHING AVE     G           64595          64798      
R360        VAN ALSTON-21ST  G           45890          50158    
```

### Find total by turnstile

```
> select ID,SCP, UNIT, STATION, ts.TOTAL_ENT as TOTAL_ENTRIES from (select *,SUM(ENTRIES) as TOTAL_ENT from entries group by SCP,UNIT) ts order by STATION;

ID          SCP         UNIT        STATION     TOTAL_ENTRIES
----------  ----------  ----------  ----------  -------------
614657      00-00-00    R248        1 AVE       101605       
614699      00-00-01    R248        1 AVE       135648       
614741      00-03-00    R248        1 AVE       24617        
614783      00-03-01    R248        1 AVE       21122        
614825      00-03-02    R248        1 AVE       34352        
614867      01-00-00    R248        1 AVE       7640         
614909      01-00-01    R248        1 AVE       20672        
614951      01-00-02    R248        1 AVE       42245        
614993      01-00-03    R248        1 AVE       80192        
615035      01-00-04    R248        1 AVE       121564 
```

### Find total for one station

```
> select ID,UNIT, STATION, ts.TOTAL_ENT as TOTAL_ENTRIES from (select *,SUM(ENTRIES) as TOTAL_ENT from entries WHERE STATION='14 ST-UNION SQ' group by UNIT) ts;

ID          UNIT        STATION         TOTAL_ENTRIES
----------  ----------  --------------  -------------
728605      R170        14 ST-UNION SQ  2675454 
```


## Other Resources
* [MTA Facts and Figures](http://web.mta.info/nyct/facts/ffsubway.htm)