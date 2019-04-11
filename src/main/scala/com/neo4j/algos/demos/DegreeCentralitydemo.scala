package com.neo4j.algos.demos

import com.neo4j.algos.GoTGraphApp

object DegreeCentralitydemo extends GoTGraphApp {

  import spark.implicits._

  val totalDegree = g.degrees
  val inDegree = g.inDegrees
  val outDegree = g.outDegrees

  totalDegree
    .join(inDegree, Seq("id"), "left")
    .join(outDegree, Seq("id"), "left")
    .na.fill(0)
    .sort($"inDegree".desc)
    .show

}
