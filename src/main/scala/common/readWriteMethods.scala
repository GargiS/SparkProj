package common

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object readWriteMethods {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  def readInputFile(spark: SparkSession,inputFilePath: String):DataFrame = {
    logger.info("readInputFile")
    val InputDataFrame =   spark.read.format("csv")
      .option("delimiter", "\t")
      .option("header","true")
      .load(inputFilePath)
    InputDataFrame
  }

  def WriteOutputToCSV(resultDF: DataFrame, fileOutPath: String, partitionedCol:String):Unit ={
    logger.info("Writing to CSV")

   resultDF.repartition(1).write
            .partitionBy("dateOfRef")
            .option("header", "true")
            .csv(fileOutPath)

  }

}