package com.neo4j.algos.demos

import com.neo4j.algos.GoTGraphApp
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types._
import org.graphframes.GraphFrame
import org.graphframes.lib.AggregateMessages

object ClosenessCentralityDemo extends GoTGraphApp {

  import spark.implicits._

  val vertices = g.vertices.withColumn("ids", struct(array(), array()))
  val cachedVertices = AggregateMessages.getCachedDataFrame(vertices)
  val g2 = GraphFrame(cachedVertices, g.edges)

  (0l to g2.vertices.count).foreach { _ =>
    val msgDst = newPathsUdf.apply(AggregateMessages.src("ids"), AggregateMessages.src("id"))
    val msgSrc = newPathsUdf.apply(AggregateMessages.dst("ids"), AggregateMessages.dst("id"))
    val agg = g2.aggregateMessages
      .sendToSrc(msgSrc)
      .sendToDst(msgDst)
      .agg(collect_set(AggregateMessages.msg).alias("agg"))
    val res = agg.withColumn("newIds", flattenUdf($"agg")).drop($"agg")
    res.show
//    val newVertices = g2.vertices.join(res, $"id", "left_outer")
//      .withColumn("mergedIds", mergePathsUdf($"ids", $"newIds"))
//    newVertices.show
  }

  def pathsType = ArrayType(
    StructType(Seq(
      StructField("id", StringType),
      StructField("distance", IntegerType)
    ))
  )

  def newPathsUdf: UserDefinedFunction = udf((paths: Row, id: String) => {
    paths.getAs[Seq[String]](0).zip(paths.getAs[Seq[Int]](1)).foldLeft(Seq((id, 1))) {
      case (acc, (pathId, distance)) if pathId != id => acc :+ (pathId, distance + 1)
      case (acc, (pathId, distance)) => acc :+ (pathId, distance)
    }
  })

  def flattenUdf: UserDefinedFunction = udf((ids: Seq[Seq[Row]]) => {
    ids.flatten.map(_.getAs[String](0))
  })

  def mergePathsUdf: UserDefinedFunction = udf(() => {

  })
}

