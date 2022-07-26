package com.github.marija

import org.apache.spark.sql.SparkSession

object day20structuredAPI extends App {
  println(s"Structured APIs with Scala version: ${util.Properties.versionNumberString}")
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")

  val df = spark.range(5).toDF("number")
  df.select(df.col("number") + 10).show(3)
  //default for show is 20

  val tinyRange = spark.range(2).toDF().collect()
  //so Collect moved the data into our own program
  val arrRow = spark.range(10).toDF(colNames="myNumber").collect()
  arrRow.take(3).foreach(println)
  arrRow.slice(2,7).foreach(println)
  println("tail")
  println(arrRow.last)
  println(arrRow.head)

  import org.apache.spark.sql.types._
  val b = ByteType

  //TODO create a DataFrame with a single column called JulyNumbers from 1 to 31
  val rangeForFirstTask = spark.range(1,32).toDF("July")
  //TODO Show all 31 numbers
  rangeForFirstTask.collect().foreach(println)
  //TODO Create another dataframe with numbers from 100 to 3100
  val rangeForSecondTask = spark.range(100, 3101).toDF()
  //TODO show last 5 numbers
  rangeForSecondTask.tail(5).foreach(println)
  val df100to3100 = rangeForFirstTask.select(rangeForFirstTask.col("July")*100)
  df100to3100.collect().reverse.take(5).foreach(println)
 //you want to collect as little as possible


}
