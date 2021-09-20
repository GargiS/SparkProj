  import org.apache.spark.sql._
  import org.apache.spark.sql.expressions._
  import org.apache.spark.sql.functions._

  import common.{readWriteMethods,commonMethods,schemaList,staticData},readWriteMethods._,commonMethods._,staticData._,schemaList._
  import org.slf4j.LoggerFactory

  object SparkScalaProj  {

    private val logger = LoggerFactory.getLogger(getClass.getName)

    def main(args: Array[String]) {

     if (args.length < 3 ) {
        logger.warn("No Argument passed")
        System.exit(1)
      }

      /** Mapping runtime arguments to InputConfig case class  */
      val inputConfig : InputConfig = InputConfig(pageType = args(1).split(",").map(_.trim)
        ,metricType = args(2).split(",").map(_.trim)
        ,timeWindow= args(3).split(",").map(_.trim)
        ,dateOfReference=args(4).trim)

      /** create SparkSession  */
      val spark = createSparkSession()
      spark.sparkContext.setLogLevel("WARN")

      logger.info("Main Started")
      import spark.implicits._

      /** read input File from location to dataframe */
      val inputData = readInputFile(spark, factFilePath)
      val lookUpData = readInputFile(spark, lookUpFilePath)

      /** Map input to case class with correct data type */
      val inputDataDS = inputData.map(r => FactTable(r.getAs[String](0).toInt, r.getAs[String](1), r.getAs[String](2).toInt)).toDF()
      val lookUpDS = lookUpData.map(r => LookUpTable(r.getAs[String](0).toInt, r.getAs[String](1))).toDF()

      /** add details from Lookup Table */
      val changedInput = mergeDF(inputDataDS, lookUpDS, "WEB_PAGEID", "inner")

      /** call function to generate and apply metrics */
      val freResultDF = applyFreqMetric(spark,changedInput,inputConfig.pageType, inputConfig.timeWindow)
      val durResultDF = applyDuration(spark,changedInput, inputConfig.pageType,inputConfig.dateOfReference)

      /** merge result dataframes */
      val resultDF = mergeDF(freResultDF, durResultDF, "USER_ID", "outer")

      /** add input DataOfReference to result dataframe */
      val resultWithRefDateDF = resultDF.withColumn("DateOfRef",lit(inputConfig.dateOfReference))

      //resultWithRefDateDF.show()
      /** Write Output  */
      WriteOutputToCSV(resultWithRefDateDF, outFilePath, inputConfig.dateOfReference)

    }

  }

