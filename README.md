# Ocean Test Task

I've chosen the parquet dataset format because:

- I have never work with it so it was kind of a challenge for me
- it's Apache Hadoop ecosystem data format, so it optimized for work with Spark and takes up less disk space

## Build and Run

Clone project on your local via Intellij IDEA.
Run [Main.scala](src/main/scala/Main.scala)

## Results

3. What is(are) the main time period(s) in the data?

```
Number of entries per day:
+----+-----+---+------+
|year|month|day|count |
+----+-----+---+------+
|2019|3    |29 |284510|
|2019|3    |28 |270724|
|2019|3    |27 |259986|
|2019|3    |26 |255248|
|2020|3    |26 |246587|
|2019|3    |25 |242440|
|2020|3    |22 |240994|
|2020|3    |27 |240757|
|2020|3    |23 |240133|
|2020|3    |25 |238563|
|2019|3    |24 |232946|
|2020|3    |24 |232081|
|2020|3    |21 |226240|
|2019|3    |23 |221948|
|2020|3    |28 |20693 |
|2019|3    |30 |20027 |
+----+-----+---+------+

The day with the biggest number of entries (the main time period) 
is 2019-3-29 (YYYY-MM-DD) with 284510 entries
```

4. Which are the top three most sparse variables?
For demonstration, I have printed out 2 tables with top 3 and top 8 sparse variables
```
TOP 3 sparse variables:
+------------+------------+----+
|column      |NotNullCount|rank|
+------------+------------+----+
|cargoDetails|333272      |1   |
|imo         |2002937     |2   |
|destination |2337298     |3   |
+------------+------------+----+

TOP 8 sparse variables:
+---------------------------+------------+----+
|column                     |NotNullCount|rank|
+---------------------------+------------+----+
|cargoDetails               |333272      |1   |
|imo                        |2002937     |2   |
|destination                |2337298     |3   |
|callSign                   |2463437     |4   |
|vesselDetails.draught      |3259452     |5   |
|vesselDetails.length       |3431682     |6   |
|vesselDetails.width        |3431682     |6   |
|epochMillis                |3473877     |8   |
|mmsi                       |3473877     |8   |
|position.latitude          |3473877     |8   |
|position.longitude         |3473877     |8   |
|navigation.navCode         |3473877     |8   |
|navigation.navDesc         |3473877     |8   |
|navigation.courseOverGround|3473877     |8   |
|navigation.heading         |3473877     |8   |
|navigation.rateOfTurn      |3473877     |8   |
|navigation.speedOverGround |3473877     |8   |
|olson_timezone             |3473877     |8   |
|vesselDetails.name         |3473877     |8   |
|vesselDetails.typeName     |3473877     |8   |
+---------------------------+------------+----+
```
5. What region(s) of the world and ocean port(s) does this data represent? Provide evidence to justify your answer.

```
Frequency for each port.name:
+-----------+-------+
|name       |count  |
+-----------+-------+
|SHANGHAI PT|3473877|
+-----------+-------+

The main port.name is SHANGHAI PT. There are 3473877 entries from there.

Frequency for each olson_timezone:
+--------------+-------+
|olson_timezone|count  |
+--------------+-------+
|Asia/Shanghai |3473877|
+--------------+-------+

The main olson_timezone is Asia/Shanghai. There are 3473877 entries from there.
```

As you can see, the main port is SHANGHAI PT (unlocode CNSHG), so the main region is China (Asia). In confirmation of
this, the main time zone is Asia/Shanghai

6. Provide a frequency tabulation of the various Navigation Codes & Descriptions (i.e., navCode & NavDesc). Optionally,
   provide any additional statistics you find interesting.</br>
   **In my opinion, one of the most useful tables is the last one, with the interval frequency of ships length**

```   
Frequency for each navigation.navDesc:
+--------------------------+-------+
|navDesc                   |count  |
+--------------------------+-------+
|Unknown                   |1357985|
|Under Way Using Engine    |1063676|
|Moored                    |554133 |
|At Anchor                 |426433 |
|Not Defined               |29330  |
|Underway Sailing          |24889  |
|Restricted Manoeuvrability|8237   |
|Not Under Command         |3471   |
|Reserved For Future Use   |2159   |
|Constrained By Her Draught|1483   |
+--------------------------+-------+
only showing top 10 rows


Frequency for each destination:
+-----------+-------+
|destination|count  |
+-----------+-------+
|null       |1136579|
|SHANGHAI   |318037 |
|SHANG HAI  |159949 |
|ZHOUSHAN   |65168  |
|ZHOU SHAN  |50691  |
|CJK        |44560  |
|YANGSHAN   |40275  |
|NEW YORK   |27296  |
|CN SHA     |27103  |
|YANG SHAN  |26683  |
+-----------+-------+
only showing top 10 rows


Frequency for each vesselDetails.typeName:
+---------------+-------+
|typeName       |count  |
+---------------+-------+
|Cargo          |1982147|
|Tanker         |367183 |
|Fishing        |321194 |
|Tug            |186250 |
|Passenger      |150087 |
|Unknown        |140610 |
|Other          |95164  |
|Law Enforcement|72544  |
|Dredging       |48475  |
|Towing         |19907  |
+---------------+-------+
only showing top 10 rows

Frequency tabulation of vesselDetails.length:
+-------------------------------------------------+------------------+
|Count of values that are included in the interval|Interval          |
+-------------------------------------------------+------------------+
|2312994                                          |[1.0; 103.1)      |
|815290                                           |[103.1; 205.2)    |
|172956                                           |[205.2; 307.3)    |
|67293                                            |[307.3; 409.4)    |
|402                                              |[409.4; 511.5)    |
|996                                              |[511.5; 613.6)    |
|157                                              |[613.6; 715.69995)|
|383                                              |[715.7; 817.8)    |
|160                                              |[817.8; 919.89996)|
|177                                              |[919.89996; 1022] |
+-------------------------------------------------+------------------+
```

7. For MMSI = 205792000, provide the following report.
   - mmsi = the MMSI of the vessel
   - timestamp = the timestamp of the last event in that contiguous series
   - Navigation Code = the navigation code (i.e., navigation.navCode)
   - Navigation Description = the navigation code description (i.e., navigation.navDesc)
   - lead time (in Milliseconds) = the time difference in milliseconds between the last and first timestamp of that particular
   - series of the same contiguous navigation codes</br>
*Need to limit the data to only the TOP 5 Navigation Codes based from the response to question 6
```
TOP 5 navCode:
+-------+-------+
|navCode|count  |
+-------+-------+
|16     |1357985|
|0      |1063676|
|5      |554133 |
|1      |426433 |
|15     |29330  |
+-------+-------+

Series of continuous events with the same Navigation Code:
+---------+-------------+------------------+----------------------+--------+------------+
|mmsi     |lastTimestamp|navigation.navCode|navigation.navDesc    |leadTime|seriesLength|
+---------+-------------+------------------+----------------------+--------+------------+
|205792000|1584950312000|5                 |Moored                |2094000 |4           |
|205792000|1585000171000|1                 |At Anchor             |48600000|48          |
|205792000|1585029441000|0                 |Under Way Using Engine|28081000|47          |
|205792000|1585107141000|5                 |Moored                |77041000|52          |
|205792000|1585129888000|0                 |Under Way Using Engine|21002000|33          |
+---------+-------------+------------------+----------------------+--------+------------+
```
8.For MMSI = 413970021, provide the same report as number 7
Do you agree with the Navigation Code(s) and Description(s) for this particular vessel?
- If you do agree, provide an explanation why you agree.
- If you do not agree, provide an explanation why do disagree. Additionally, if you do not agree, what would you change it
to and why?
```
 Series of continuous events with the same Navigation Code:
+---------+-------------+------------------+------------------+---------+------------+
|mmsi     |lastTimestamp|navigation.navCode|navigation.navDesc|leadTime |seriesLength|
+---------+-------------+------------------+------------------+---------+------------+
|413970021|1553890569000|16                |Unknown           |591975000|393         |
+---------+-------------+------------------+------------------+---------+------------+
```
I am agree with the Navigation Code and Description for this particular vessel because
navCode=16 matches the navDesc=Unknown and there are no entries with given MMSI, but 
with another navCode (and navDesc)