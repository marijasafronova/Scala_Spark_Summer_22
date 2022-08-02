package com.github.marija

import org.apache.spark.sql.functions.{col, expr, lpad, rpad}

object day24exercise extends App {

  val spark = SparkUtil.getSpark("exercise")
  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"

  val df = SparkUtil.getCSVwithView(spark, filePath)
  spark.sql("SELECT Description, initcap(Description) FROM dfTable")
    .show(3, false)

  spark.sql(
    """
      |SELECT Description, Country,
      |rpad(Country, 22, '_'),
      |lpad(Country, 22, '_'),
      |lpad(rpad(Country, 15+(CHAR_LENGTH(Country))/2, '_'), 30, '_') as ___Country___
      |FROM dfTable
      |""".stripMargin).show(1000, false)

  val materialss = Seq("wood", "metal")


  df.select(
    col("Description"),
    col("Country"),
    rpad(col("Country"), 30 - "United Kingdom".length/2, "_").as("Country_"),
    lpad(col("Country"), 30 - "United Kingdom".length/2, "_").as("_Country_"),
    lpad(rpad(col("Country"), 22, "_"), 30, "_").as("__Country__"),
    //expr("lpad(rdpad(Country, 15+int(CHAR_LENGTH(Country))/2), '_') as ___Country___")
  ).show(10,false)


  val materials = Seq("wood", "metal", "ivory")
  val regexString = materials.map(_.toUpperCase).mkString("|")
  println(regexString)


//  df.select(
//    regexp_replace(col("Description"), regexString, "material").alias("Material_Desc"),
//    col("Description"))
//    .show(10,false)
//
//  df.select(translate(col("Description"), "LEET", "1337"), col("Description"))
//    .show(2)
//
//  val regexString1 = simpleColors.map(_.toUpperCase).mkString("(", "|", ")")
//  // the | signifies OR in regular expression syntax
//  df.select(
//    regexp_extract(col("Description"), regexString1, 1).alias("color_clean"),
//    col("Description")).show(2)

  val containsBlack = col("Description").contains("BLACK")
  val containsWhite = col("DESCRIPTION").contains("WHITE")
  df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
    .where("hasSimpleColor")
    .select("Description").show(3, false)

  spark.sql(
    """
      |SELECT Description FROM dfTable
      |WHERE instr(Description, 'BLACK') >= 1 OR instr(Description, 'WHITE') >= 1
      |""".stripMargin)
      .show(5, false)

  val simpleColors = Seq("black", "white", "red", "green", "blue")
  val selectedColumns = simpleColors.map(color => {
    col("Description").contains(color.toUpperCase).alias(s"is_$color")
  }):+expr("*") // could also append this value
  df.select(selectedColumns:_*).show(10,false)
  df.select(selectedColumns:_*).where(col("is_white").or(col("is_red")))
    .select("Description").show(3, false)

  df.select(selectedColumns.head, selectedColumns(3), selectedColumns.last, col("Description"))
    .show(3, false)

  val numbers = Seq(1,5,6,20,5)


}
