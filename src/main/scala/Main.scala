import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object Main {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Ocean Test Task Session")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") // turn off INFO and WARN logs

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    //    val filePath = "./src/main/parquet_data/part-00000-91b4ffd0-5234-4ade-9b7f-a8a66789a0a7-c000.snappy.parquet"
    val filePath = "./src/main/parquet_data/*"

    val oceanDF = spark.read.parquet(filePath).cache()

    // 3.What is(are) the main time period(s) in the data?
    findMainTimePeriod(oceanDF)
    //
    //4. Which are the top three most sparse variables?
    findSparseVars(oceanDF)
    findSparseVars(oceanDF, 8)

    //
    // 5. What region(s) of the world and ocean port(s) does this data represent?
    findMainValue(oceanDF, "port.name")
    findMainValue(oceanDF, "olson_timezone")
    //
    // 6. Provide a frequency tabulation of the various Navigation Codes & Descriptions (i.e., navCode & NavDesc).
    // Optionally, provide any additional statistics you find interesting.
    getFrequency(oceanDF, "navigation.navDesc")
    getFrequency(oceanDF, "destination")
    getFrequency(oceanDF, "vesselDetails.typeName")
    findFrequencyByIntervals(oceanDF, "vesselDetails.length", 10)
    // 7. For MMSI = 205792000, provide the following report:
    //    - mmsi = the MMSI of the vessel
    //    - timestamp = the timestamp of the last event in that contiguous series
    //    - Navigation Code = the navigation code (i.e., navigation.navCode)
    //    - Navigation Description = the navigation code description (i.e., navigation.navDesc)
    //    - lead time (in Milliseconds) = the time difference in milliseconds between the last and
    //    first timestamp of that particular
    //    - series of the same contiguous navigation codes
    val report = createReport(oceanDF, 205792000, "navigation.navCode",
      "navigation.navDesc", 5)
    println("Series of continuous events with the same Navigation Code:")
    report.show(false)
    // 8.For MMSI = 413970021, provide the same report as number 7
    //    Do you agree with the Navigation Code(s) and Description(s) for this particular vessel?
    //    - If you do agree, provide an explanation why you agree.
    //    - If you do not agree, provide an explanation why do disagree. Additionally, if you do not agree, what would you change it
    //    to and why?
    val secondReport = createReport(oceanDF, 413970021, "navigation.navCode",
      "navigation.navDesc", 5)
    println("Series of continuous events with the same Navigation Code:")
    secondReport.show(false)

  }

  /** Finds time period with the biggest number of entries (the main time period)
   *
   * Gets dataframe with dwells entries, adds column with date from epochMillis column,
   * groups entries by day, finds and prints out number of entries per each day.
   * From all days finds the main (with the biggest number of entries)
   *
   * @param df the DataFrame with dwells info
   */
  def findMainTimePeriod(df: DataFrame): Unit = {
    val dfWithDate = df.withColumn("dateTime", from_unixtime($"epochMillis" / 1000))

    // TODO use window($"dateTime", "1 day") instead of group by year, month, and day
    val numberPerDay = dfWithDate.groupBy(year(dfWithDate("dateTime")).alias("year"),
      month(dfWithDate("dateTime")).alias("month"),
      dayofmonth(dfWithDate("dateTime")).alias("day"))
      .count()
      .orderBy(desc("count"))

    println("Number of entries per day:")
    numberPerDay.show(false)

    val mainDate = numberPerDay.first()
    println(s"The day with the biggest number of entries (the main time period) is " +
      s"${mainDate.getAs("year")}-${mainDate.getAs("month")}-${mainDate.getAs("day")} " +
      s"(YYYY-MM-DD) with ${mainDate.getAs("count")} entries")
  }


  /** Returns the list of column names of the data frame
   *
   * @param dt            data frame schema
   * @param path          name of the column/nested column
   * @param listOfColumns temp list of column names
   * @return ListBuffer[String] list of column names
   */
  def getColumns(dt: DataType, path: String = "",
                 listOfColumns: ListBuffer[String] = ListBuffer[String]()): ListBuffer[String] = {
    dt match {
      case s: StructType =>
        s.fields.foreach(f => getColumns(f.dataType, path + "." + f.name, listOfColumns))
      case _ =>
        listOfColumns += path
    }
    listOfColumns.map(columnName => columnName.substring(1)) // delete '.' from the begin of the column name
  }

  /** Finds top most sparse variables
   *
   * From df gets a list of all columns, counts the number of non-null values in each column, and
   * prints out the top most sparse variables (columns with the least number of non-null values)
   *
   * @param df  the DataFrame with dwells info
   * @param top the Integer number of top values that users need (3 by default)
   */
  def findSparseVars(df: DataFrame, top: Int = 3): Unit = {

    var countOfVarsDF: DataFrame = Seq.empty[(String, Long)]
      .toDF("column", "NotNullCount")
    val columnsList = getColumns(df.schema)
    for (columnName <- columnsList) {
      val b = df
        .filter(df(columnName).isNotNull)
        .select(lit(columnName).alias("column"), count(columnName).alias("NotNullCount"))
      countOfVarsDF = countOfVarsDF.unionAll(b)
    }
    countOfVarsDF = countOfVarsDF.orderBy("NotNullCount")
    countOfVarsDF.show(false)
    countOfVarsDF = countOfVarsDF.withColumn("rank", rank().over(Window.orderBy("NotNullCount")))
    println(s"TOP $top sparse variables:")
    countOfVarsDF.filter($"rank" <= top).show(false)
  }


  /** Finds main value of the column
   *
   * Gets dataframe with dwells entries and counts number of entries for each group.
   * Prints out the main group (group with the biggest number of entries)
   *
   * @param df         the DataFrame with dwells info
   * @param columnName the String column name to find main value
   */
  def findMainValue(df: DataFrame, columnName: String): Unit = {
    val portsDF = getFrequency(df, columnName)
    val mainRow = portsDF.first()
    println(s"The main $columnName is ${mainRow.get(0)}. " +
      s"There are ${mainRow.getAs("count")} entries from there.")
  }

  /** Returns frequency DataFrame (tabulation) of column
   *
   * Gets dataframe with dwells entries, groups by column, and counts number
   * of entries for each group in this column
   *
   * @param df         the DataFrame with dwells info
   * @param columnName the String column name to find main value
   * @return DataFrame with values and number of entries for each value
   */
  def getFrequency(df: DataFrame, columnName: String): DataFrame = {
    val frequencyDF = df.groupBy(columnName).count().orderBy(desc("count"))
    println(s"\nFrequency for each $columnName:")
    frequencyDF.show(10, truncate = false)
    frequencyDF
  }

  /** Finds interval frequency DataFrame (tabulation) of column
   *
   * Cleans df(columnName) from null and zero values, in for-loop counts the intervals (based on min and max values in
   * df(columnName) and numberOfIntervals) and finds the number of entries, that included in every interval.
   * Number of entries for the last interval count separately (outside the loop), because it is closed interval, but
   * not half-opened as previous. Checks if all values from df were distributed into intervals (sum of values before
   * distribution must be equal to the sum after) and prints out interval frequency tabulation
   *
   * @param df                the DataFrame with dwells info
   * @param columnName        the String column name to find main value
   * @param numberOfIntervals the Integer number of intervals
   * @return DataFrame with values and number of entries
   */
  def findFrequencyByIntervals(df: DataFrame, columnName: String, numberOfIntervals: Int): Unit = {
    val dfWithoutNull = df.filter(df(columnName).isNotNull)
      .filter(df(columnName) =!= 0)
      .groupBy(columnName)
      .count()

    val nestedColumnName = columnName.split("\\.").last
    val minLength = dfWithoutNull.agg(min(dfWithoutNull(nestedColumnName))).first().get(0).asInstanceOf[Long]
    val maxLength = dfWithoutNull.agg(max(dfWithoutNull(nestedColumnName))).first().get(0).asInstanceOf[Long]
    val interval = (maxLength - minLength) / numberOfIntervals.asInstanceOf[Float]

    var frequencyDF: DataFrame = null
    for (interval_index <- 1 until numberOfIntervals) {
      val intervalStartPoint = minLength + interval * (interval_index - 1)
      val intervalEndPoint = intervalStartPoint + interval
      // add number of entries in half-opened intervals
      val entriesInInterval = dfWithoutNull
        .filter(dfWithoutNull(nestedColumnName) >= intervalStartPoint)
        .filter(dfWithoutNull(nestedColumnName) < intervalEndPoint)
        .agg(sum("count").alias("Count of values that are included in the interval"))
        .withColumn("Interval", lit("[" + intervalStartPoint + "; " + intervalEndPoint + ")"))
      interval_index match {
        case 1 => frequencyDF = entriesInInterval
        case _ => frequencyDF = frequencyDF.unionAll(entriesInInterval)
      }
    }

    val lastIntervalStartPoint = minLength + interval * (numberOfIntervals - 1)
    val lastIntervalEndPoint = maxLength // == lastIntervalStartPoint + interval
    // add number of entries in last closed interval
    val entriesInLastInterval = dfWithoutNull
      .filter(dfWithoutNull(nestedColumnName) >= lastIntervalStartPoint)
      .filter(dfWithoutNull(nestedColumnName) <= lastIntervalEndPoint)
      .agg(sum("count").alias("Count of values that are included in the interval"))
      .withColumn("Interval", lit("[" + lastIntervalStartPoint + "; " + lastIntervalEndPoint + "]"))
    frequencyDF = frequencyDF.unionAll(entriesInLastInterval)

    // Check
    // Sum of entries before division into intervals
    val initialEntriesSumS = dfWithoutNull
      .agg(sum("count"))
      .first()
      .get(0)
    // and after division into intervals
    val entriesSum = frequencyDF
      .agg(sum("Count of values that are included in the interval"))
      .first()
      .get(0)
    // should be the same
    assert(initialEntriesSumS == entriesSum)


    println(s"Frequency tabulation of $columnName:")
    frequencyDF.show(false)
  }

  /** Unfinished function that provides report about series of continuous events
   *
   * Finds series of continuous events in df(columnName) (series where column value does not change), provides
   * info about duration of the series and final state
   *
   * @param df                   the DataFrame with dwells info
   * @param mmsi                 Maritime Mobile Service Identity number
   * @param columnName           the column for series searching
   * @param additionalColumnName column with additional information that must be added to the final answer
   * @param top                  the number of top elements by which the data will be filtered
   * @return DataFrame with series of continuous events with the same values in the df(columnName):
   */
  def createReport(df: DataFrame, mmsi: Long, columnName: String, additionalColumnName: String, top: Int): DataFrame = {
    val nestedColumnName = columnName.split("\\.").last
    val topNavCodes = getFrequency(df, columnName).select(nestedColumnName).collect().map(_ (0)).toList.take(top)
    val mmsiDF = df
      .filter($"mmsi" === mmsi)
      .filter(df(columnName).isin(topNavCodes: _*))
      .orderBy("epochMillis")
      .withColumn("nestedColumn", df(columnName))
      .withColumn("nestedAdditionalColumn", df(additionalColumnName))

    //    println(s"Entries with mmsi=$mmsi in chronology")
    //    mmsiDF.show(500, truncate = false)

    // Create empty DataFrame for the answer (series of continuous events with the same Navigation Code)
    val seriesDFColumns = Seq("mmsi", "lastTimestamp", columnName, additionalColumnName, "leadTime", "seriesLength")
    var seriesDF: DataFrame = Seq
      .empty[(Long, Long, Long, String, Long, Long)]
      .toDF(seriesDFColumns: _*)

    // set initial values of iteration variables
    var previousRow: Row = mmsiDF.head() //take the first row from the data frame
    var seriesLength = 1
    var firstSeriesTime: Long = previousRow.getAs("epochMillis")

    for (row <- mmsiDF.collect().drop(1)) {
      // continuous series
      if (row.getAs("nestedColumn") == previousRow.getAs("nestedColumn") ||
        row.getAs("nestedAdditionalColumn") == previousRow.getAs("nestedAdditionalColumn")) {
        seriesLength += 1
        previousRow = row
      }
      // interrupted by the next series
      else {
        val lastSeriesTime = previousRow.getAs("epochMillis").asInstanceOf[Long]
        val leadTime = lastSeriesTime - firstSeriesTime
        val column: Long = previousRow.getAs("nestedColumn")
        val additionalColumn: String = previousRow.getAs("nestedAdditionalColumn")
        val seriesRow = Seq((mmsi, lastSeriesTime, column, additionalColumn, leadTime, seriesLength))
          .toDF(seriesDFColumns: _*)
        seriesDF = seriesDF.union(seriesRow) // add row with info about series to the result data frame

        // reset iteration variables
        seriesLength = 1
        firstSeriesTime = row.getAs("epochMillis")
        previousRow = row
      }
    }
    // add unfinished series (last series that was not interrupted)
    val lastSeriesTime = previousRow.getAs("epochMillis").asInstanceOf[Long]
    val leadTime = lastSeriesTime - firstSeriesTime
    val column: Long = previousRow.getAs("nestedColumn")
    val additionalColumn: String = previousRow.getAs("nestedAdditionalColumn")
    val seriesRow = Seq((mmsi, lastSeriesTime, column, additionalColumn, leadTime, seriesLength))
      .toDF(seriesDFColumns: _*)
    seriesDF = seriesDF.union(seriesRow)

    seriesDF
  }

}
