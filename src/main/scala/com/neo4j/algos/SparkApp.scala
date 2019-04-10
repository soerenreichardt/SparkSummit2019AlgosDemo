package com.neo4j.algos

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

abstract class SparkApp extends App {

  implicit val spark: SparkSession = {
    val session = SparkSession
      .builder()
      .config(new SparkConf(true))
      .appName("Spark AI Demo")
      .master("local[*]")
      .getOrCreate()
    session.sparkContext.setLogLevel("error")
    session
  }

}
