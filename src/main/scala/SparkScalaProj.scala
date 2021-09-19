
  import org.apache.spark.sql._
  import org.apache.spark.sql.expressions._
  import org.apache.spark.sql.functions._


  import common.{readWriteMethods,commonMethods,schemaList,staticData},readWriteMethods._,commonMethods._,staticData._
  import org.slf4j.LoggerFactory

  import scala.collection.mutable.ListBuffer
  // USER_ID	EVENT_DATE	WEB_PAGEID WEBPAGE_TYPE

  //case class FactTable(USER_ID: Int, EVENT_DATE: String, WEB_PAGEID: Int)
  //case class LookUpTable(WEB_PAGEID: Int, WEBPAGE_TYPE: String)

  object SparkScalaProj extends schemaList {

    private val logger = LoggerFactory.getLogger(getClass.getName)

    def main(args: Array[String]) {

      val arg_length = args.length

      if (arg_length < 3 ) {
        logger.warn("No Argument passed")
        System.exit(1)
      }

      val inputConfig : InputConfig = InputConfig(pageType = args(1).split(",").map(_.trim)
                                 ,metricType = args(2).split(",").map(_.trim)
                                 ,timeWindow= args(3).split(",").map(_.trim)
                                 ,dateOfReference=args(4).trim)

   /*   val pageType = "news, movies".split(",").map(_.trim) //'news, movies'
      val metricType = "fre dur".split(",".trim) // 'fre, dur'
      val timeWindow = "365, 730, 1460, 2920".trim.split(",") // '365, 730, 1460, 2920'
      val dateOfReference = "12/10/2019"

    */

      val spark = createSparkSession()
      spark.sparkContext.setLogLevel("WARN")

      logger.info("Main Started")
      import spark.implicits._

      // read Input Files
      val inputData = readInputFile(spark, factFilePath)
      val lookUpData = readInputFile(spark, lookUpFilePath)

      val inputDataDS = inputData.map(r => FactTable(r.getAs[String](0).toInt, r.getAs[String](1), r.getAs[String](2).toInt)).toDF()
      val lookUpDS = lookUpData.map(r => LookUpTable(r.getAs[String](0).toInt, r.getAs[String](1))).toDF()

      val changedInput = mergeDF(inputDataDS, lookUpDS, "WEB_PAGEID", "inner")

      val pageType = "news, movies".trim.split(",").map(_.trim) //'news, movies'
      val metricType = "fre dur".split(",".trim) // 'fre, dur'
      val timeWindow = "365, 730, 1460, 2920".trim.split(",") // '365, 730, 1460, 2920'
      val dateOfReference = "12/10/2019"


      // applying metrics
      val freResultDF = applyFreqMetric(spark,changedInput,inputConfig.pageType, inputConfig.timeWindow)
      val durResultDF = applyDuration(spark,changedInput, inputConfig.pageType,inputConfig.dateOfReference)

      // merge Metric dataFrames
      val resultDF = mergeDF(freResultDF, durResultDF, "USER_ID", "outer")

      // write output to csv
      WriteOutputToCSV(resultDF, outFilePath, inputConfig.dateOfReference)

    }


  }

