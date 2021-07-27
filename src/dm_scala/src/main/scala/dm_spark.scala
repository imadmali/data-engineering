/*
// use spark-shell --master local[2] to run spark commands in scala
val textFile = spark.read.textFile("data/fact_table.csv")
textFile.count()

val spark = SparkSession.builder.master("local").appName("Data Manipulation Scala").getOrCreate()
val data = spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("data/fact_table.csv")
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, sum, max}
// import org.apache.spark.sql.functions._

object dm_scala {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Data Manipulation Scala").getOrCreate()

    val data = spark.read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("data/fact_table.csv")

    data.show()
    data.filter(col("id")==="A").show()
    data.groupBy("id").agg(count("v0").alias("v0_count"), sum("v0").alias("v0_sum"), max("v0").alias("v0_max")).show()
    spark.stop()
  }
}
