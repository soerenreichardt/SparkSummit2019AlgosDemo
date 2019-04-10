package com.neo4j.algos.demos

import com.neo4j.algos.GoTGraphApp

object TriangleCountDemo extends GoTGraphApp(Some(1)) {

  import spark.implicits._

  val result = g.triangleCount.run
  result
    .sort($"count".desc)
    .filter($"count" > 0)
    .show

}
