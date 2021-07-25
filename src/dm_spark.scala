// val data = spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("data/dim_table.csv")
// use spark-shell to run spark commands in scala
val data = spark.read
  .format("csv")
  .option("sep", ",")
  .option("inferSchema", "true")
  .option("header", "true")
  .load("data/fact_table.csv")

data.show()
data.filter(col("id")==="A").show()
data.groupBy("id").agg(count("v0").alias("v0_count"), sum("v0").alias("v0_sum"), max("v0").alias("v0_max")).show()
