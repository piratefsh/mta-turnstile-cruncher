# MTA Turnstile Data Cleaner


## What this does
From a database generated from the [mta-turnstile-scraper](https://github.com/piratefsh/mta-turnstile-scraper), find the entries and exits for each row. 

Converts columns `ENTRIES` to `CUM_ENTRIES` and `EXITS` to `CUM_EXITS`. Updates calculated entries and exits in `ENTRIES` and `EXITS` columns respectively.

Raw data only has cumulative numbers per row, so some math and massaging was needed to get the exact numbers at each logged time slot. Does it by finding all entries per unique turnstile and subtracting the `CUM_ENTRIES` from a previous entry. Also ignores negative entries/exits and entries with no previous `CUM_ENTRIES/CUM_EXITS`.

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

This is pulled from Sept 2015 data only, so just a relatively small dataset.


###Top 10 most popular stations for Sept 2015

Query finds total by station and sorts by `TOTAL_ENTRIES`

```
> select ID, UNIT, STATION, LINENAME, ts.TOTAL_ENT as TOTAL_ENTRIES from (select *, SUM(ENTRIES) as TOTAL_ENT from entries group by UNIT) ts order by TOTAL_ENTRIES DESC limit 10;

ID          UNIT        STATION          LINENAME     TOTAL_ENTRIES
----------  ----------  ---------------  -----------  -------------
635141      R011        42 ST-PA BUS TE  ACENQRS1237  7515283142   
586808      R080        57 ST-7 AVE      NQR          7322653178   
714660      R084        59 ST-COLUMBUS   1ABCD        4287013743   
713284      R033        42 ST-TIMES SQ   1237ACENQRS  3833759862   
726965      R044        BROOKLYN BRIDGE  456JZ        3689538466   
705651      R028        FULTON ST        2345ACJZ     3335250387   
680062      R453        23 ST-6 AVE      FM           3079427013   
729663      R131        23 ST            6            3071870495   
773955      R110        FLATBUSH AVE     25           2958008384   
716443      R452        72 ST            123          2829892542  
```

### Top 10 Stations on the 1 line

42nd st wins by a huge margin. Not sure how different 42 ST- PA BUS is from 42 ST- TIMES SQ. Considering consolidating them.

```
UNIT        STATION          LINENAME     TOTAL_ENTRIES  TOTAL_EXITS
----------  ---------------  -----------  -------------  -----------
R011        42 ST-PA BUS TE  ACENQRS1237  7515283142     6224158924 
R084        59 ST-COLUMBUS   1ABCD        4287013743     4811284800 
R033        42 ST-TIMES SQ   1237ACENQRS  3833759862     3280405602 
R452        72 ST            123          2829892542     2907651868 
R030        CHAMBERS ST      123          2178364226     1773333113 
R189        CHRISTOPHER ST   1            1920636346     738990120  
R032        42 ST-TIMES SQ   1237ACENQRS  1870596495     1218086552 
R105        14 ST            123FLM       1496914943     1593372963 
R273        145 ST           1            1358092539     1976354079 
R159        116 ST-COLUMBIA  1            1207900247     255070842 
```

### Stations on the G line, in order of decresing popularity

The '7' exclusion is there to filter out the weird 42nd st stop that apparently has 'G' in it.

```

> select UNIT, STATION,LINENAME,ts.TOTAL_ENT as TOTAL_ENTRIES,ts.TOTAL_EX as TOTAL_EXITS from (select *,SUM(EXITS) as TOTAL_EX,SUM(ENTRIES) as TOTAL_ENT from entries where instr(LINENAME, 'G') and not instr(LINENAME, '7')  group by UNIT) ts order by TOTAL_ENTRIES DESC limit 30;

UNIT        STATION     LINENAME    TOTAL_ENTRIES  TOTAL_EXITS
----------  ----------  ----------  -------------  -----------
R359        COURT SQ    EMG         248900062      18377314   
R256        NASSAU AV   G           178524401      57885167   
R269        BEDFORD/NO  G           172430677      50741833   
R204        CHURCH AVE  FG          166686368      41822070   
R258        4 AVE       DFGMNR      52380918       38808209   
R220        CARROLL ST  FG          42016153       24971189   
R268        METROPOLIT  GL          39410701       25254671   
R129        BERGEN ST   FG          36063923       22484146   
R217        HOYT/SCHER  ACG         29809049       30488426   
R288        7 AV-PARK   FG          25362691       7996743    
R317        CLINTON-WA  G           19619335       4252381    
R286        MYRTLE-WIL  G           19350748       12598318   
R241        15 ST-PROS  FG          18728069       8269590    
R299        BROADWAY    G           14289913       10596625   
R287        CLASSON AV  G           13911699       13593579   
R239        GREENPOINT  G           13700077       8417595    
R318        FULTON ST   G           12306053       12674774   
R289        FT HAMILTO  FG          10381010       3556658    
R270        SMITH-9 ST  FG          10210066       7663262    
R316        FLUSHING A  G           7076375        6954122    
R360        VAN ALSTON  G           5435475        6156631
```

### Find total by turnstile

```
> select ID,SCP, UNIT, STATION, ts.TOTAL_ENT as TOTAL_ENTRIES from (select *,SUM(ENTRIES) as TOTAL_ENT from entries group by SCP,UNIT) ts order by STATION;

ID          SCP         UNIT        STATION     TOTAL_ENTRIES
----------  ----------  ----------  ----------  -------------
614657      00-00-00    R248        1 AVE       10173346     
614699      00-00-01    R248        1 AVE       54404349     
614741      00-03-00    R248        1 AVE       369340725    
614783      00-03-01    R248        1 AVE       1405477      
614825      00-03-02    R248        1 AVE       4819128 
```

### Find total for one station

```
> select ID,UNIT, STATION, ts.TOTAL_ENT as TOTAL_ENTRIES from (select *,SUM(ENTRIES) as TOTAL_ENT from entries WHERE STATION='14 ST-UNION SQ' group by UNIT) ts;

ID          UNIT        STATION         TOTAL_ENTRIES
----------  ----------  --------------  -------------
728605      R170        14 ST-UNION SQ  1310473486 
```


## Other Resources
* [MTA Facts and Figures](http://web.mta.info/nyct/facts/ffsubway.htm)