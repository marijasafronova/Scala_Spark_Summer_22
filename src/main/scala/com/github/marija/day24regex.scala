package com.github.marija

import org.apache.spark.sql.functions.{col, initcap, lit, lower, lpad, ltrim, regexp_replace, rpad, rtrim, trim, upper}

object day24regex extends App {


  val spark = SparkUtil.getSpark("Strings")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  val df = SparkUtil.getCSVwithView(spark, filePath)

  df.select(col("Description"),
    initcap(col("Description")).alias("DescInitCap"))
    .show(2, false)

  spark.sql("SELECT Description, initcap(Description) FROM dfTable")
    .show(3, false)

  df.select(col("Description"),
    lower(col("Description")),
    upper(lower(col("Description")))).show(3, false)

  spark.sql("SELECT Description, lower(Description), Upper(lower(Description)) FROM dfTable").show(3, false)
  df.select(
    col("CustomerId"),
    ltrim(lit(" HELLO ")).as("ltrim"),
    rtrim(lit(" HELLO ")).as("rtrim"),
    trim(lit(" HELLO ")).as("trim"),
    lpad(lit("HELLO"), 3, " ").as("lp"),
    lpad(rpad(lit("HELLO"), 5, "*"), 5, "*").as("pad5starts"),
    rpad(lit("HELLO"), 10, " ").as("rp")
  ).show(2)

  spark.sql(
    """
      |SELECT
      |CustomerId,
      | ltrim(' HELLLOOOO '),
      | rtrim(' HELLLOOOO '),
      | trim(' HELLLOOOO '),
      | lpad('HELLOOOO ', 3, ' '),
      | rpad('HELLOOOO ', 10, ' ')
      |FROM dfTable
      |""".stripMargin)

  val simpleColors = Seq("black", "white", "red", "green", "blue")
  val regexString = simpleColors.map(_.toUpperCase).mkString("|")
  // the | signifies `OR` in regular expression syntax
  df.select(
    regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
    col("Description")).show(5, false)

  spark.sql(
    """
      |SELECT
      | regexp_replace(Description, 'BLACK|WHITE|RED|GREEN|BLUE', 'COLOR') as
      | color_clean, Description
      |FROM dfTable
      |""".stripMargin)
    .show(5,false)



}
