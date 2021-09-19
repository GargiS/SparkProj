
import org.apache.spark.sql._
import org.scalatest.FlatSpec

class sparkTest extends FlatSpec {

  //class sparkTest extends sparkScalaFinal
  lazy val spark = SparkSession.builder().appName("helloSpark").config("master", "local").getOrCreate()

  def fixture = new {
    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("mock_InputData.csv")
  }
}

 // behaviour of "Spark Common"

  /*it should "create a session" in {
    val inputConfig : InputConfig = InputConfig(env = "dev",targetDB = "pg")
    val spark = SparkCommon.createSparkSession(inputConfig).get
}*/

// check for invalid input arguments
 /* it should "throw invalid environment exception" in {

    val inputConfig : InputConfig = InputConfig(env = "abc",targetDB = "pg")

    assertThrows[InvalidEnvironmentException] {
      val spark = SparkCommon.createSparkSession(inputConfig).get
    }

  }*/
