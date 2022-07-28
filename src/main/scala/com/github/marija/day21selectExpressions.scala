package com.github.marija
import org.apache.spark.sql.functions.{expr, lit}

object day21selectExpressions extends App {

  val spark = SparkUtil.getSpark("SelectExpressions")

  val flightPath = "src/resources/flight-data/json/2015-summary.json"

  val df = spark.read.format("json")
    .load(flightPath)

  df.show(3)

  val statDf = df.describe() // statistical description
  statDf.show()

  df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)
 //we are not returning result, just showing it

  df.selectExpr(
    "*", // include all original columns
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
    .show(10)

  //how could we get the row(s) that represents withinCountry flights?

  df.selectExpr("DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME").show()
  df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

  //new dataframe with the column wih all 42s
  df.select(expr("*"), lit(42).as("Answer")).show(2)

  df.selectExpr("*", "42 as theAnswer").show(3)

  //adding columns
  df.withColumn("numberOne", lit(33))
    .withColumn("numberOne", lit(44))
    .show(2)

  df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
  df.withColumn("BigCount", expr("count*100+100"))
    .show(2)

  df.withColumn("Destination", expr("DEST_COUNTRY_NAME")).columns

  df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns

  val dfWithLongColName = df.withColumn(
    "This Long Column-Name",
    expr("ORIGIN_COUNTRY_NAME"))

  dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`")
    .show(2)
}
