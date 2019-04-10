package com.neo4j.algos.utils

import org.apache.spark.sql.DataFrame
import org.graphframes.GraphFrame

object GotGraphBuilder {

  def createGoTGraphFrame(nodesDf: DataFrame, edgeDf: DataFrame): GraphFrame = {
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
