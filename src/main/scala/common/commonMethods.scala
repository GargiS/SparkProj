package common

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, current_date, datediff, lit, row_number, to_date, to_timestamp}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer
import org.slf4j.LoggerFactory
object commonMethods {

  private val logger = LoggerFactory.getLogger(getClass.getName)
  def createSparkSession(): SparkSession =
  {
    logger.info("Creating Session..")
    val spark = SparkSession.builder()
      .appName("Spark Schema Demo")
      .master("local[3]")
      .getOrCreate()

    spark
  }

  def mergeDF(inputDS: DataFrame, lookupDS: DataFrame,column :String,joinTyp :String):DataFrame = {
    logger.info("Merging DF")
    val changedInput = inputDS.join(lookupDS,inputDS(column) === lookupDS(column) ,joinTyp)
      .drop(inputDS.col(column))

    changedInput
  }


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
