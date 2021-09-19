sbt assembly
spark-submit --master local --class SparkScalaTest.SparkScalaProj "news, movies" "fre, dur" "365, 730, 1460, 2920" "12/10/2019"
target/scala-2.11/spark-test-app-2.11-0.1.jar
