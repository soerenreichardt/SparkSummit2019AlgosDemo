package com.neo4j.algos

import com.neo4j.algos.utils.GoTDataReader.{readEdgeData, readNodeData}
import com.neo4j.algos.utils.GotGraphBuilder.createGoTGraphFrame

abstract class GoTGraphApp(val bookNr: Option[Int] = None) extends SparkApp {

  private val nodes = readNodeData(bookNr)
  private val edges = readEdgeData(bookNr)

  val g = createGoTGraphFrame(nodes, edges)

}
