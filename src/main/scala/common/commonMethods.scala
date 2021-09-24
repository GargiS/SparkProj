  package common

  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql._

  import scala.collection.mutable.ListBuffer
  import org.slf4j.LoggerFactory
  object commonMethods {

    private val logger = LoggerFactory.getLogger(getClass.getName)

    val eventFilter = (eventColumn:Column, eventType:String) => eventColumn === eventType
    val dateFilter = (dateColumn:Column, daysToCheck: Int) => {
      val formattedDate = to_date(to_timestamp(dateColumn, "dd/MM/yyyy HH:mm"),"dd-MM-yyyy")
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
     */
    def applyDuration(spark:SparkSession,inputDF:DataFrame, pageType: Array[String],referenceDate:String ):DataFrame = {

      logger.info("Generate Duration Metrics..")
      //1. Filter records as per pageType and with event_date less than reference date
      //2. Group by userId and generate metric (datediff between reference date and event_date)

      import spark.implicits._
      val userIdDF = inputDF.select(col("USER_ID")).distinct
      val outputDF = pageType.map(et => inputDF
             .withColumn("dateOfRef", to_date(to_timestamp(lit(referenceDate), "dd/MM/yyyy"), "dd-MMM-yyyy"))
             .withColumn("newEvent_date", to_date(to_timestamp($"EVENT_DATE", "dd/MM/yyyy HH:mm"), "dd-MMM-yyyy"))
             .filter( eventFilter(col("WEBPAGE_TYPE"),et) && col("newEvent_date") < col("dateOfRef"))
             .withColumn("dateDiff",datediff($"dateOfRef",$"newEvent_date"))
             .groupBy(col("USER_ID"))
             .min("dateDiff")
             .withColumnRenamed("min(dateDiff)","pageview_" + et.trim + "_" + "dur")
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
     */

    def applyFreqMetric(spark:SparkSession,dateChangedInputDF: DataFrame, pageType: Array[String],  timeWindow: Array[String]): DataFrame = {

      logger.info("Generate Frequency Metric ..")
      val userIdDF = dateChangedInputDF.select(col("USER_ID")).distinct

      // 1. Filter records as per pageType and if event date lies within timeWindow
      // 2. Group by user and calculate count for the metric (datediff between current_date and event_date)

     val outputDF = pageType.flatMap(et => timeWindow.map(tw => dateChangedInputDF
               .withColumn("formattedDate",to_date(to_timestamp(col("EVENT_DATE"), "dd/MM/yyyy HH:mm"),"dd-MMM-yyyy"))
              .withColumn("daysDiff",datediff(current_date(),col("formattedDate" )))
              .filter(eventFilter(col("WEBPAGE_TYPE"),et) && col("daysDiff") <= lit(tw))
              .groupBy(col("USER_ID"))
              .count
              .withColumnRenamed("count","pageview" + "_" + et + "_fre_" + tw)
              .withColumnRenamed("USER_ID","USER_ID_TMP")))
              .foldLeft(userIdDF)((userIdDF, inputDF) => userIdDF.join(inputDF,userIdDF("USER_ID") === inputDF("USER_ID_TMP"),"outer")
              .drop("USER_ID_TMP")).orderBy("USER_ID").na.fill("")

      outputDF
    }
  }
