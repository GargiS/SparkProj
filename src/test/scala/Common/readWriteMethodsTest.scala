package Common

import common.commonMethods.applyDuration
import org.apache.spark.sql.{DataFrame, SparkSession}
import common.readWriteMethods._
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class readWriteMethodsTest extends FunSuite with BeforeAndAfterAll{

  lazy val  spark = SparkSession.builder().appName("helloSpark").master("local").getOrCreate()

  val testFile = "/Users/gargi/Documents/InterviewQuestions/SparkScalaTest/src/main/resources/fact.csv"

  override def beforeAll():Unit ={
    val df : DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema","true")
      .csv(testFile)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("create a dataframe from inputFile") {
    val outDF = readInputFile(spark,testFile)
    assert ( outDF.count() > 1)
    }

}


