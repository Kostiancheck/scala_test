import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{dayofmonth, desc, from_unixtime, month, year}

object Main {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Spark SQL basic example")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") // turn off INFO and WARN logs

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val filePath = "./src/main/parquet_data/*"
    val oceanDF = spark.read.parquet(filePath).cache()

    findMainTimePeriod(oceanDF)
    //findSparseVars(oceanDF)
  }

  /** Finds time period with the biggest number of entries (main time period)
   *
   * Gets dataframe with dwells entries, groups entries by day,
   * finds and prints out number of entries per each day.
   * From all days finds the main (with the biggest number of entries)
   *
   * @param df the DataFrame with dwells info
   */
  def findMainTimePeriod(df: DataFrame): Unit = {
    val dfWithDate = df.withColumn("dateTime", from_unixtime($"epochMillis" / 1000))

    val numberPerDay = dfWithDate.groupBy(year(dfWithDate("dateTime")).alias("year"),
      month(dfWithDate("dateTime")).alias("month"),
      dayofmonth(dfWithDate("dateTime")).alias("day"))
      .count()
      .orderBy(desc("count"))

    println("Number of entries per day:")
    numberPerDay.show(false)

    val mainDate = numberPerDay.first()
    print(s"The day with the biggest number of entries (the main time period) is " +
      s"${mainDate.getAs("year")}-${mainDate.getAs("month")}-${mainDate.getAs("day")} " +
      s"(YYYY-MM-DD) with ${mainDate.getAs("count")} entries")
  }

  /** Unfinished function that finds top most sparse variables
   *
   * Gets dataframe with dwells entries, groups entries by day,
   * finds and prints out number of entries per each day.
   * From all days finds the main (with the biggest number of entries)
   *
   * @param df the DataFrame with dwells info
   * @param top the Integer number of top values that users need (3 by default)
   */
  def findSparseVars(df: DataFrame, top: Int = 3): Unit = {
    val dfCountPerColumn = df.describe().filter($"summary" === "count")
    println("Number of non-null values in each column")
    dfCountPerColumn.show(false)
    // TODO
    // 1. transpose dfCountPerColumn
    // 2. order by count
    // 3. take first 3 (by default) values
    // 4.(optional) if two of the first three rows have the same value, then take the fourth, fifth etc.
    // until there are 3 different values
  }
}