# Ocean Test Task
I've chosen the parquet dataset format because:
- I have never work with it so it was kind of a challenge for me
- it's Apache Hadoop ecosystem data format, so it optimized for work with Spark and takes up less disk space
## Build and Run

Clone project on your local via Intellij IDEA
Run [Main.scala](src/main/scala/Main.scala)
## Results
Result of the first task:
```bash
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
