package com.neo4j.algos.demos

import com.neo4j.algos.GoTGraphApp

object PageRankDemo extends GoTGraphApp(Some(1)) {

  import spark.implicits._

  val results = g.pageRank.resetProbability(0.15).maxIter(20).run()
  results.vertices.orderBy($"pageRank".desc).show
}
