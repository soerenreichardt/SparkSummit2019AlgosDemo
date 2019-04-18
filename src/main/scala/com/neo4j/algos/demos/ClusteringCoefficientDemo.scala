package com.neo4j.algos.demos

import com.neo4j.algos.GoTGraphApp

object ClusteringCoefficientDemo extends GoTGraphApp {

  import spark.implicits._

  val triangleCount = g.triangleCount.run
  val degrees = g.degrees

  val joinedDFs = triangleCount.join(degrees, Seq("id"))
  val result = joinedDFs.withColumn("cc", ($"count" * 2) / ($"degree" * ($"degree" - 1)))
  result.show

}
