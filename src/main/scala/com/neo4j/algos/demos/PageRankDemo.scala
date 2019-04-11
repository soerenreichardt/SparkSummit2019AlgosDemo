package com.neo4j.algos.demos

import com.neo4j.algos.GoTGraphApp

object PageRankDemo extends GoTGraphApp {

  import spark.implicits._

  val results = g.pageRank.resetProbability(0.15).tol(0.01).run()
  results.vertices.orderBy($"pageRank".desc).show
}
