package com.github.marija

import org.apache.spark.sql.types._

object day20basicStructureOperations extends App {

  val spark = SparkUtil.getSpark("BasicSpark")

  val flightPath = "src/resources/flight-data/json/2015-summary.json"

  val df = spark.read.format("json")
    .load(flightPath)

  df.show(5)
  println(df.schema)
  df.printSchema()

  val myManualSchema = StructType(Array(
    StructField("DEST_COUNTRY_NAME", StringType, true),
    StructField("ORIGIN_COUNTRY_NAME", StringType, true),
    StructField("count", LongType, false,
      Metadata.fromJson("{\"hello\":\"world\"}"))
  ))

  println(df.columns.mkString(","))
  val firstRow =  df.first()
  println(firstRow)
  val last5Rows = df.tail(5)
  val lastRow = df.tail(1)(0)

  df.coalesce(1)
    .write
    .option("header", "true")
    .option("sep", ",")
    .mode("overwrite")
    .csv("src/resources/csv/flight_summary_2015.csv")
}
