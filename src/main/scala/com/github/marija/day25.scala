package com.github.marija

import org.apache.spark.sql.functions.{col, current_date, datediff, current_timestamp, date_add, date_sub, lit, months_between, to_date}

object day25 extends App {
  val spark = SparkUtil.getSpark("Dates and timestamps")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  val df = SparkUtil.getCSVwithView(spark, filePath)


  val dateDF = spark.range(10)
    .withColumn("today", current_date())
    .withColumn("now", current_timestamp())
  dateDF.createOrReplaceTempView("dateTable")

  dateDF.show(10, false)
  dateDF.printSchema()

  dateDF.select(date_sub(col("today"), 5),
    date_add(col("today"), 5))
    .show(1)

  dateDF.withColumn("week_ago", date_sub(col("today"), 7))
    .select(datediff(col("week_ago"), col("today"))).show(1)
  dateDF.select(
    to_date(lit("2016-01-01")).alias("start"),
    to_date(lit("2017-05-22")).alias("end"))
    .select(months_between(col("start"), col("end"))).show(1)


  //exercise
  //TODO open March 1st , 2011

  val marchDFfilePath = "src/resources/retail-data/by-day/2011-03-01.csv"
  val anotherDF = SparkUtil.getCSVwithView(spark, marchDFfilePath)

  //Add new column with current date

  anotherDF.select("InvoiceDate")
    .withColumn("date", current_date())
    .withColumn("time", current_timestamp())
    .withColumn("dDifference", datediff(col("time"), col("InvoiceDate")))
    .withColumn("mDifference", months_between(col("time"), col("InvoiceDate")))
    .show(1)

  //Add new column with current timestamp
  //add new column which contains days passed since InvoiceDate (here it is March 1, 2011 but it could vary)
  //add new column with months passed since InvoiceDate

}
