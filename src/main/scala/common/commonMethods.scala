package common

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, current_date, datediff, lit, row_number, to_date, to_timestamp}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer
import org.slf4j.LoggerFactory
object commonMethods {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  val eventFilter = (eventColumn:Column, eventType:String) => eventColumn === eventType
  val dateFilter = (dateColumn:Column, daysToCheck: Int) => {
    val formattedDate = to_date(to_timestamp(dateColumn, "dd/MM/yyyy HH:mm"),"dd-MMM-yyyy")
    val diffInDays = datediff(current_date(),formattedDate)
    diffInDays <= daysToCheck
  }

  /***
    createSparkSession : function creates and return SparkSession
    (parameters can be added later)
   ***/
  def createSparkSession(): SparkSession =
  {
    logger.info("Creating Session..")
    val spark = SparkSession.builder()
      .appName("Spark Schema Demo")
      .master("local[3]")
      .getOrCreate()

    spark
  }

  /***
   * mergeDF : function to join/merge 2 dataframes
   * @param inputDS1 , inputDS2 : input Dataframes to be joined
   * @param column :  column Name to be used in join Condition
   * @param joinTyp : the type of join to be applied
   * @return : merged/joined DataFrame
   */
  def mergeDF(inputDS1: DataFrame, inputDS2: DataFrame,column :String,joinTyp :String):DataFrame = {
    logger.info("Merging DF")
    val changedInput = inputDS1.join(inputDS2,inputDS1(column) === inputDS2(column) ,joinTyp)
      .drop(inputDS2.col(column))

    changedInput
  }

  /***
   * applyDuration : function to apply Duration metrics
   * @param spark : current SparkSession
   * @param inputDF : Input DataFrame
   * @param pageType : Type of page ( input parameters)
   * @param referenceDate : Date of Reference ( input parameters)
   * @return  result DataFrame
   */
  def applyDuration(spark:SparkSession,inputDF:DataFrame, pageType: Array[String],referenceDate:String ):DataFrame = {


    import spark.implicits._
    val userIdDF = inputDF.select(col("USER_ID")).distinct
    val outputDF = pageType.map(et => inputDF
      .withColumn("dateFormatted", to_date(to_timestamp($"EVENT_DATE", "dd/MM/yyyy HH:mm"), "dd-MMM-yyyy"))
      .withColumn("ref_date",to_date(to_timestamp(lit(referenceDate), "dd/MM/yyyy"), "dd-MMM-yyyy"))
      .filter(eventFilter(col("WEBPAGE_TYPE"),et) && col("dateFormatted") < col("ref_date"))
      .withColumn("diff",datediff(col("ref_date"),col("dateFormatted")))
      .groupBy(col("USER_ID"))
      .min("diff")
      .withColumnRenamed("min(diff)","pageview_" + et.trim + "_" + "dur")
      .withColumnRenamed("USER_ID","USER_ID_TMP"))
      .foldLeft(userIdDF)((userIdDF, inputDF) => userIdDF.join(inputDF,userIdDF("USER_ID") === inputDF("USER_ID_TMP"),"outer")
        .drop("USER_ID_TMP")).orderBy("USER_ID")

    outputDF
  }


  /***
   * applyFreqMetric : Function to generate and apply  metric.
   * @param spark : current Spark Session
   * @param dateChangedInputDF : input DataFrame
   * @param pageType : Type of page ( input parameters)
   * @param timeWindow : Time ranges (in days) to be applied
   * @return : resultant DataFrame
   */
  def applyFreqMetric(spark:SparkSession,dateChangedInputDF: DataFrame, pageType: Array[String],  timeWindow: Array[String]): DataFrame = {

    val userIdDF = dateChangedInputDF.select(col("USER_ID")).distinct
   val outputDF = pageType.flatMap(et => timeWindow.map(tw => dateChangedInputDF
      .filter(eventFilter(col("WEBPAGE_TYPE"),et) && dateFilter(col("EVENT_DATE"),tw.toInt))
      .groupBy(col("USER_ID"))
      .count.na.fill(0)
      .withColumnRenamed("count","pageview" + "_" + et + "_fre_" + tw)
      .withColumnRenamed("USER_ID","USER_ID_TMP")))
      .foldLeft(userIdDF)((userIdDF, inputDF) => userIdDF.join(inputDF,userIdDF("USER_ID") === inputDF("USER_ID_TMP"),"outer")
        .drop("USER_ID_TMP")).orderBy("USER_ID").na.fill("")

    outputDF
  }
}
