package com.arkenstone

/**
  * Created by Abhijeet on 4/16/17.
  * A spark code to read data from hive tables, and join based on composite keys and store back in hive
  */

import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive._
import org.apache.spark.sql._

object PopulationDataNow {

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark
    val sparkConf = new SparkConf()
      .setAppName("PopulationDataNow")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.autoBroadcastJoinThreshold", (100 * 1024 * 1024).toString)
      .set("spark.sql.planner.externalSort", "true")
      .set("spark.sql.shuffle.partitions", 3.toString)

   //   .setJars(Seq(System.getProperty("user.dir") + "/target/scala-2.10/sparktest.jar"))

    val sc = new SparkContext(sparkConf)

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    // importing hive-context utilities and sql math functions
    import hiveContext.implicits._
    import org.apache.spark.sql.functions._


    // population Table operations

    //loading hive table STG_WeatherTable.projected_population" casting datatypes
    val populationTable = hiveContext.sql("select distinct (upper(city)) as CITY, " +
      "cast (estimated_population as bigint) as EP, state from STG_WeatherTable.projected_population").repartition(column("state")).cache()


    /* debugging population Table
    populationTable.printSchema()
    populationTable.collect.foreach(println)
    println(populationTable.count())
    */


    //WeatherTable operations
    //loading hive table STG_WeatherTable.projected_population" casting datatypes

    val WeatherTable = hiveContext.sql("select wban, hourlyprecip, stationtype, station_name, state_province, county, country " +
    "from STG_WeatherTable.weather_data")


    //casting datatype and removing row containing junk values

    val Wtable = WeatherTable.withColumn("hourlyprecip", 'hourlyprecip.cast("Float")).na.fill(0.0)

    // if you want to read schema-->Wtable.printSchema()

    // Adding hourly precipitation for a city to calculate total precipitation in May 2015
    val wtableSum = Wtable.groupBy($"county",$"state_province").agg(sum($"hourlyprecip").alias("TotalPrec")).repartition(col("state_province"))

/* debug snippet
          wtableSum.printSchema()
          wtableSum.collect.foreach(println)
          println("original  " + Wtable.count())
          println("new  " + wtableSum.count())
*/

  val joinDf = populationTable.join( wtableSum, (populationTable("state") === wtableSum("state_province"))
    && (populationTable("CITY") === wtableSum("county")) ,"left_outer").drop("county").drop("state_province")
    .sort($"TotalPrec".desc)
/*
  // debug snippet
      joinDf.printSchema()
      joinDf.collect.foreach(println)
*/


  //Displaying data in hive table
  joinDf.registerTempTable("joinDfTable")
  hiveContext.sql("DROP TABLE STG_WeatherTable.out")
  hiveContext.sql("CREATE TABLE IF NOT EXISTS STG_WeatherTable.out AS SELECT * from joinDfTable")

  sc.stop()
}
}
