package com.neo4j.algos

import com.neo4j.algos.utils.GoTDataReader.{readEdgeData, readNodeData}
import org.apache.spark.sql.DataFrame
import org.graphframes.GraphFrame

abstract class GoTGraphApp(val bookNr: Option[Int] = None) extends SparkApp {

  private val nodes = readNodeData(bookNr)
  private val edges = readEdgeData(bookNr)

  val g = createGoTGraphFrame(nodes, edges)

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
