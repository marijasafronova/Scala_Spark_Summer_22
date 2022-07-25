package com.github.marija

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, window}

object day19streaming extends App {
  println(s"Streaming with Scala version: ${util.Properties.versionNumberString}")
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")
  //partition count for local count
  spark.conf.set("spark.sql.shuffle.partitions", "5")
 //just regular dataframe
// val staticDataFrame = spark.read.format("csv")
//   .option("header", "true")
//   .option("inferSchema", "true") //we are letting Spark figure out the type of data in our CSVs
//   // .load("src/resources/retail-data/by-day/*.csv")//we are loading anything
//   .load("src/resources/retail-data/by-day/2010-12-01.csv")//we are loading anything


  //csv reading documentation
val staticDataFrame = spark.read
  .option("header", "true")
  .option("inferSchema", "true") //we are letting Spark figure out the type of data in our CSVs
  .csv("src/resources/retail-data/by-day/*.csv")//we are loading anything, does not work
  //.csv("src/resources/retail-data/by-day/2010-12-01.csv")
  staticDataFrame.createOrReplaceTempView("retail_data")
  val staticSchema = staticDataFrame.schema
  println(staticSchema.toArray.mkString("\n"))

  println(s"We got ${staticDataFrame.count()} rows of data!") //count action goes across all partitions
// batch query
  staticDataFrame
    .selectExpr(
      "CustomerId",
      "(UnitPrice * Quantity) as total_cost",
      "InvoiceDate")
    .groupBy(
      col("CustomerId"), window(col("InvoiceDate"), "1 day"))
    .sum("total_cost")
    .show(5)

  //streaming query
  val streamingDataFrame = spark.readStream
    .schema(staticSchema) //we provide the schema that we got from static read
    .option("maxFilesPerTrigger", 1)
    .format("csv")
    .option("header", "true")
  //.load("src/resources/retail-data/by-day/2010-12-01.csv")
    .load("src/resources/retail-data/by-day/*.csv")

  println(streamingDataFrame.isStreaming)

//  val purchaseByCustomerPerHour = streamingDataFrame
//    .selectExpr(
//      "CustomerId",
//      "(UnitPrice * Quantity) as total_cost",
//      "InvoiceDate")
//    .groupBy(
//      col("CustomerId"),  window(col("InvoiceDate"), "1 day"))
//    .sum("total_cost")
//
//  purchaseByCustomerPerHour.writeStream
//    .format("memory") // memory = store in-memory table
//    .queryName("customer_purchases") // the name of the in-memory table
//    .outputMode("complete") // complete = all the counts should be in the table
//    .start()
//
//  spark.sql("""
//              SELECT *
//              FROM customer_purchases
//              ORDER BY `sum(total_cost)` DESC
//              """)
//    .show(5)

}
