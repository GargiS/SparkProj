package common

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, current_date, datediff, lit, row_number, to_date, to_timestamp}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer
import org.slf4j.LoggerFactory
object commonMethods {

  private val logger = LoggerFactory.getLogger(getClass.getName)

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
      .drop(inputDS1.col(column))

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

    var dataframeList: ListBuffer[org.apache.spark.sql.DataFrame] = ListBuffer()

    pageType.foreach { pType =>

      val columnName = "pageview_" + pType.trim + "_" + "dur"

      val tempDF = inputDF.filter($"webPage_Type" === pType)
        .withColumn("rn", row_number() over Window.partitionBy(inputDF("USER_ID"), inputDF.col("WEBPAGE_TYPE")).orderBy($"EVENT_DATE"))
        .filter($"rn" === 1).drop($"rn")
        .withColumn("dateOfRef", to_date(to_timestamp(lit(referenceDate), "dd/MM/yyyy"), "dd-MMM-yyyy"))
        .withColumn("newEvent_date", to_date(to_timestamp($"EVENT_DATE", "dd/MM/yyyy HH:mm"), "dd-MMM-yyyy"))
        .withColumn(columnName,datediff($"dateOfRef",$"newEvent_date"))
        .select("USER_ID", columnName).distinct

     dataframeList += tempDF
    }

    val outputDF = dataframeList.reduce(_.join(_, Seq("USER_ID"), "full_outer"))
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

    var dataframeList: ListBuffer[org.apache.spark.sql.DataFrame] = ListBuffer()

    dateChangedInputDF.printSchema()

    pageType.foreach { pType =>
      timeWindow.foreach { timeX =>
        val columnName = "pageview_" + pType.trim + "_" + "fre" + "_" + timeX.trim

        import spark.implicits._

        val tempDF =  dateChangedInputDF.filter($"WEBPAGE_TYPE" === pType)
          .withColumn("newEvent_date",to_date(to_timestamp($"EVENT_DATE", "dd/MM/yyyy HH:mm"),"dd-MMM-yyyy"))
          .withColumn("daysDiff",datediff(current_date(),$"newEvent_date" ))
          .filter($"daysDiff" <= timeX.toInt)
          .groupBy($"USER_ID").agg(count("*") as columnName)
          .select("USER_ID",columnName)

       // tempDF.show()
        dataframeList += tempDF
      }
    }

    val freResultDF = dataframeList.reduce(_.join(_, Seq("USER_ID"), "full_outer"))
    freResultDF

  }
}
