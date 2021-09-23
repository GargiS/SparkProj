  import org.apache.spark.sql._
  import org.apache.spark.sql.expressions._
  import org.apache.spark.sql.functions._
  import common.{commonMethods, readWriteMethods, schemaList, staticData}
  import readWriteMethods._
  import commonMethods._
  import staticData._
  import schemaList._
  import org.slf4j.LoggerFactory

  object SparkScalaProj  {

    private val logger = LoggerFactory.getLogger(getClass.getName)

    def main(args: Array[String]) {

      /** Input Argument needed =  <pageType:metricType:TimeWindow:dateOfReference> */
     if (args.length < 3 ) {
        logger.warn("No Argument passed")
        System.exit(1)
      }

      /** Mapping runtime arguments to InputConfig case class  */

      val inputConfig : InputConfig = InputConfig(pageType = args(0).split(",").map(_.trim)
        ,metricType = args(1).split(",").map(_.trim)
        ,timeWindow= args(2).split(",").map(_.trim)
        ,dateOfReference=args(3).trim)

      /** create SparkSession  */
      val spark = createSparkSession()
      spark.sparkContext.setLogLevel("WARN")

      logger.info("Main Started")
      import spark.implicits._

      /** read input File from location to dataframe */
      val inputDataDF = readInputFile(spark, factFilePath)
      val lookUpDataDF = readInputFile(spark, lookUpFilePath)

      /** add details from Lookup Table */
      val changedInput = mergeDF(inputDataDF, lookUpDataDF, "WEB_PAGEID", "inner")

      /** call function to generate and apply metrics */
      val freResultDF = applyFreqMetric(spark,changedInput,inputConfig.pageType, inputConfig.timeWindow)
      val durResultDF = applyDuration(spark,changedInput, inputConfig.pageType,inputConfig.dateOfReference)

      /** merge result dataframes */
      val resultDF = mergeDF(freResultDF, durResultDF, "USER_ID", "outer")

      /** add input DataOfReference to result dataframe */
      //val resultWithRefDateDF = resultDF.withColumn("DateOfRef",lit(inputConfig.dateOfReference))

     // println("Details.....")
      //resultDF.show()

      /** Write Output  */
      WriteOutputToCSV(resultDF,outFilePath, inputConfig.dateOfReference)

    }

  }
