package com.github.marija

import org.apache.spark.sql.functions.col

object day22sensitivity extends App {

  val spark = SparkUtil.getSpark("MoreTransformations")

  val flightPath = "src/resources/flight-data/json/2015-summary.json"

  val df = spark.read.format("json")
    .load(flightPath)

  spark.conf.set("spark.sql.caseSensitive", true)
  df.show(5)
  df.drop("ORIGIN_COUNTRY_NAME").show(5)

  df.printSchema()
  //our count is already long
  //so we will make it double
  val dfWith3Counts = df.withColumn("count2", col("count").cast("int"))
  .withColumn("count3", col("count").cast("double "))

  





}
