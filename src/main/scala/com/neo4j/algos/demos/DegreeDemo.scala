package com.neo4j.algos.demos

import com.neo4j.algos.GoTGraphApp

object DegreeDemo extends GoTGraphApp {

  import spark.implicits._

  val outDegree = g.outDegrees
  outDegree
    .sort($"outDegree".desc)
    .show

}
