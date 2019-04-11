package com.neo4j.algos

import com.neo4j.algos.utils.GoTDataReader.{readEdgeData, readNodeData}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes.GraphFrame

abstract class GoTGraphApp(val bookNr: Option[Int] = None) extends App {

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

  val g = createGoTGraphFrame(nodes, edges)

  private val nodes = readNodeData(bookNr)
  private val edges = readEdgeData(bookNr)

  private def createGoTGraphFrame(nodesDf: DataFrame, edgeDf: DataFrame): GraphFrame = {
    val preprocessedEdges = preprocessEdges(edgeDf)

    GraphFrame(nodesDf, preprocessedEdges)
  }

  private def preprocessEdges(edgeDf: DataFrame): DataFrame = {
    val reversedEdgesDf = edgeDf
      .withColumn("newSrc", edgeDf.col("dst"))
      .withColumn("newDst", edgeDf.col("src"))
      .drop("src", "dst")
      .withColumnRenamed("newSrc", "src")
      .withColumnRenamed("newDst", "dst")

    edgeDf.union(reversedEdgesDf)
  }

}
