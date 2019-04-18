package com.neo4j.algos

import com.neo4j.algos.utils.GoTDataReader.{readEdgeData, readNodeData}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes.GraphFrame

abstract class GoTGraphApp extends App {

  final val season: Option[Int] = Some(2)
  final val medium: Medium = Series()

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

  private def nodes = readNodeData(season, medium.path)
  private def edges = readEdgeData(season, medium.path)

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

  trait Medium { def path: String }
  case class Book() extends Medium { override val path = "book/asoiaf-book"}
  case class Series() extends Medium { override val path = "series/got-s"}

}
