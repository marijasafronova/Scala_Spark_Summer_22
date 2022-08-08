package com.github.marija

import org.apache.spark.sql.functions.udf

object day27exercise extends App {

  val spark = SparkUtil.getSpark("27exercise")

  val df = spark.range(-40, 120+1).toDF("farenheits")

  def farenheitsToCelsius (n: Double):Double = (n-32)*0.5556

  val convertation = udf(farenheitsToCelsius(_:Double):Double)

  spark.udf.register("farenheitsToCelsius", farenheitsToCelsius(_:Double):Double)
  df.createOrReplaceTempView("dfTable")

  spark.sql(
    """
      |SELECT *,
      |farenheitsToCelsius(farenheits)
      |FROM dfTable
      |""".stripMargin)
    .show()
}
