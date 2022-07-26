package com.github.marija

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.types.{LongType, IntegerType, DoubleType, BooleanType, StringType, StructField, StructType}
object day21rowsAndTransformations extends App {

  val spark = SparkUtil.getSpark("RowDataframeTransformation")

  val myRow = Row("Hello", null, 1, false, 3.1415926)
  println(myRow(0))
  println(myRow(0).asInstanceOf[String]) // String
  println(myRow.getString(0))
  val myGreeting = myRow.getString(0) // String
  println(myGreeting)

  println(myRow.getInt(2)) // Int
  val myDouble = myRow.getInt(2).toDouble
  val myPi = myRow.getDouble(4)
  println(myPi)

  println(myRow.schema)

  val flightPath = "src/resources/flight-data/json/2015-summary.json"

  //so a
  val df = spark.read.format("json")
    .load(flightPath)

  df.createOrReplaceTempView("dfTable")
  df.show(5)

  val myManualSchema = new StructType(Array(
    StructField("some", StringType, true),
    StructField("col", StringType, true),
    StructField("names", LongType, false)))

  val myRows = Seq(Row("hello", null, 1L),
  Row("Sparky", "some strings", 151L),
  Row("Valdis", "some_strings", 161L),
  Row(null, "some_strings", 161L))


  val myRDD = spark.sparkContext.parallelize(myRows)
  val myDf = spark.createDataFrame(myRDD, myManualSchema)
  myDf.show()

  //val alsoDF = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")

  df.select("DEST_COUNTRY_NAME").show(2)

  val newDF = df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME")
  newDF.show(3)

  val sqlWay = spark.sql("""
    SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME
    FROM dfTable
    LIMIT 10
    """)
  sqlWay.show(3)

  //4 approaches

  df.select(
    df.col("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"),
//    'DEST_COUNTRY_NAME,
//    $"DEST_COUNTRY_NAME",
    expr("DEST_COUNTRY_NAME"))
    .show(2)

//  df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
//  df.select(expr("DEST_COUNTRY_NAME as destination").alias("my_dest"))
//    .show(2)

  val newManualSchema = new StructType(Array(
    StructField("food_name", StringType, true),
    StructField("quantity", IntegerType, true),
    StructField("vegan", BooleanType , true),
    StructField("price", DoubleType, true)))

  val newRows = Seq(
    Row("Pineapple", 3, true, 8.99),
    Row("Watermelon", 10, true, 3.99),
    Row("Melon", 4, true, 5.99)
    )

  val newRDD = spark.sparkContext.parallelize(newRows)
  val foodFrame = spark.createDataFrame(newRDD, newManualSchema)

  val foodDF = foodFrame.select("food_name", "quantity")
  foodDF.show()

}
