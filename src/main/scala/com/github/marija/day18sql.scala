package com.github.marija
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, max}

object day18sql extends App {
  println(s"Reading SCVs with Scala version: ${util.Properties.versionNumberString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")

  val flightData2015 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/resources/flight-data/csv/2015-summary.csv")
  println(s"We have ${flightData2015.count()} rows of data")

  flightData2015.createOrReplaceTempView("flight_data_2015")
  //of we want to use sql syntax we create sql view
  val sqlWay = spark.sql("""
    SELECT DEST_COUNTRY_NAME, count(1)
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
    """)

  val dataFrameWay = flightData2015
    .groupBy("DEST_COUNTRY_NAME")
    .count()
      sqlWay.explain
      dataFrameWay.explain

  sqlWay.show(10)
  dataFrameWay.show(10)

  val flightData2014 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/resources/flight-data/csv/2014-summary.csv")

  flightData2014.createOrReplaceTempView("flight_data_2014")

   //order by flight count
  val sqlFlights = spark.sql("""
    SELECT DEST_COUNTRY_NAME, sum(count) as flight
    FROM flight_data_2014
    GROUP BY DEST_COUNTRY_NAME
    ORDER BY flight DESC
    """)
  sqlFlights.show(10)


//two approaches to do the same thing
  flightData2015.select(max("count")).show(1)
  spark.sql("SELECT max(count) from flight_data_2015").toDF().show(1)

  val maxSql = spark.sql("""
    SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
    ORDER BY sum(count) DESC
    LIMIT 5
    """)

  flightData2015
    .groupBy("DEST_COUNTRY_NAME")
    .sum("count")
    .withColumnRenamed("sum(count)", "destination_total")
    .sort(desc("destination_total"))
    .limit(5)
    .show()

  case class Flight(DEST_COUNTRY_NAME: String,
                    ORIGIN_COUNTRY_NAME: String,
                    count: BigInt)
  //one of those dark corners of Scala called implicits, which is a bit magical

//  implicit val enc: Encoder[Flight] = Encoders.product[Flight]
//  val flightsDF = spark.read
//    .parquet("/data/flight-data/parquet/2010-summary.parquet/")
//  val flights = flightsDF.as[Flight]
//  val flightsArray = flights.collect() //now we have local storge of our flights
//  //now we can use regular Scala methods
////parquet is compressed csv data

//  println(s"We have information on ${flightsArray.length} flights")
//  val sortedFlights= flightsArray.sortBy(_.count)
//  println(sortedFlights.take(5).mkString("\n"))
  //  flightData2015
//    .groupBy("DEST_COUNTRY_NAME")
//    .sum("count")
//    .withColumnRenamed("sum(count)", "destination_total")
//    .sort(desc("destination_total"))
//    .toDF()
//    .write
//    .format("CSV")
//    .mode("overwrite")
//    .option("sep","\t")
//    .save("src/resources/flight-data/csv/top_destinations_2015.tsv")

}
