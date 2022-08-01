package com.github.marija

object day24exercise extends App {

  val spark = SparkUtil.getSpark("exercise")
  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"

  val df = SparkUtil.getCSVwithView(spark, filePath)
  spark.sql("SELECT Description, initcap(Description) FROM dfTable")
    .show(3, false)

  spark.sql(
    """
      |SELECT
      |Description, Country,
      |lpad('Country', 15, '_'),
      |rpad('Country ', 15, '_')
      |FROM dfTable
      |""".stripMargin).show(10, false)

  val materials = Seq("wood", "metal")



}
