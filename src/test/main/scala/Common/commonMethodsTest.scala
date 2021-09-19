package Common

import common.commonMethods._
import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class commonMethodsTest extends FunSuite with BeforeAndAfterAll{

lazy val  spark:SparkSession =   SparkSession.builder.appName("HelloSpark")
    .master("local")
    .getOrCreate()

  val testFile = "/SparkScalaTest/src/main/resources/mockFile.csv"

  /*
  override def beforeAll():Unit ={
    val df : DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema","true")
      .csv("mock_InputData.csv")

  }

  override def afterAll(): Unit = {
    spark.stop()
  } */

 test("create a session") {
    val spark2 = createSparkSession()
  }

  test("verify frequency metrics"){

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")
  val df : DataFrame = spark.read.option("header", "true").option("inferSchema","true")
                              .option("delimiter","\t").csv(testFile)

   val outputDF = applyFreqMetric(spark,df,Array("news"),  Array("365"))
    val userIdDetails = outputDF
      .filter(outputDF("USER_ID").equalTo("710"))
       .select("pageview_news_fre_365").collectAsList()
    val value = userIdDetails.get(0)(0)

   assert(value == 1)
  }

  test("merge two dataframes") {
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")
    val df : DataFrame = spark.read.option("header", "true").option("inferSchema","true")
      .option("delimiter","\t").csv(testFile)

    val outputDF = applyDuration(spark,df,Array("news"),  "12/10/2021")

    val userIdDetails = outputDF
      .filter(outputDF("USER_ID").equalTo("710"))
      .select("pageview_news_dur").collectAsList()
    val value = userIdDetails.get(0)(0)

    assert(value == 262)

  }
}
