#! bin/bash
# 1. from src/dm_scala folder: sbt package
# 2. from data-manipulation folder: source src/dm_scala/run.sh

spark-submit \
  --class "dm_scala" \
  --master local[2] \
  src/dm_scala/target/scala-2.12/data-manipulation-scala_2.12-1.0.jar
