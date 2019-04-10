package com.neo4j.algos.demos

import org.apache.spark.sql.functions._
import com.neo4j.algos.GoTGraphApp

object LabelPropagationDemo extends GoTGraphApp(Some(1)) {

  import spark.implicits._

  val result = g.labelPropagation.maxIter(10).run
  result
    .sort($"label")
    .groupBy($"label")
    .agg(collect_list("id").alias("people"))
    .sort(size($"people").desc)
    .show(false)

}
