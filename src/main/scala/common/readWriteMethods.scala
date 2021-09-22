package common

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object readWriteMethods {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  /***
    readInputFile : function to read input CSV FILE and return dataframe
   ***/
  def readInputFile(spark: SparkSession,inputFilePath: String):DataFrame = {
    logger.info("readInputFile")
    val InputDataFrame =   spark.read.format("csv")
      .option("delimiter", "\t")
      .option("header","true")
      .load(getClass.getResource(inputFilePath).toString())
    InputDataFrame
  }

  /***
    WriteOutputToCSV : function to write dataframe to CSV FILE
   ***/

  def WriteOutputToCSV(resultDF: DataFrame, fileOutPath: String, partitionedCol:String):Unit ={

    logger.info("Writing to CSV" + fileOutPath)

   resultDF.repartition(1).write
          //  .partitionBy("dateOfRef")
            .option("header", "true")
            .mode(SaveMode. Overwrite)
            .csv(fileOutPath)


  }

}
