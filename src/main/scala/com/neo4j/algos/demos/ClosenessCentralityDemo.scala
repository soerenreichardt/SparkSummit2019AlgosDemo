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

  val vertices = g.vertices.withColumn("ids", array(struct($"id", lit(1).as("distance"))))
  val cachedVertices = AggregateMessages.getCachedDataFrame(vertices)
  var g2 = GraphFrame(cachedVertices, g.edges)

  (0l to g2.vertices.count).foreach { _ =>
    val msgDst = newPathsUdf.apply(AggregateMessages.src("ids"), AggregateMessages.src("id"))
    val msgSrc = newPathsUdf.apply(AggregateMessages.dst("ids"), AggregateMessages.dst("id"))
    val agg = g2.aggregateMessages
      .sendToSrc(msgSrc)
      .sendToDst(msgDst)
      .agg(collect_set(AggregateMessages.msg).alias("agg"))
    val res = agg.withColumn("newIds", flattenUdf($"agg")).drop($"agg")
    val newVertices = g2.vertices.join(res, Seq("id"), "left_outer")
      .withColumn("mergedIds", mergePathsUdf($"ids", $"newIds", $"id"))
      .drop("ids", "newIds")
      .withColumnRenamed("mergedIds", "ids")
    val cachedNewVertices = AggregateMessages.getCachedDataFrame(newVertices)
    g2 = GraphFrame(cachedNewVertices, g2.edges)
  }

  g2.vertices
    .withColumn("closeness", closenessUdf($"ids"))
    .sort($"closeness".desc)
    .show(false)

  case class PathsType(id: String, distance: Int)
  val pathsType = ArrayType(
    StructType(Seq(
      StructField("id", StringType, true),
      StructField("distance", IntegerType, true))
    )
  )

  def newPathsUdf: UserDefinedFunction = udf[Seq[PathsType], Seq[Row], String]((paths: Seq[Row], id: String) => {
    paths.foldLeft(Seq.empty[PathsType]) {
      case (acc, pType) if pType.getAs[String](0) != id => acc :+ PathsType(pType.getAs[String](0), pType.getAs[Int](1) + 1)
      case (acc, pType) => acc :+ PathsType(pType.getAs[String](0), pType.getAs[Int](1))
    }
  })

  def flattenUdf: UserDefinedFunction = udf((ids: Seq[Seq[Row]]) => {
    ids.flatten.map(row => PathsType(row.getAs[String](0), row.getAs[Int](1)))
  })

  def mergePathsUdf: UserDefinedFunction = udf[Seq[PathsType], Seq[Row], Seq[Row], String]((ids: Seq[Row], newIds: Seq[Row], id: String) => {
    (ids ++ newIds)
      .map(row => PathsType(row.getAs[String](0), row.getAs[Int](1)))
      .filter(_.id == id)
      .sortBy(_.distance)
  })

  def closenessUdf: UserDefinedFunction = udf[Double, Seq[Row]]((ids: Seq[Row]) => {
    val nodes = ids.length
    val totalDistance = ids.map(_.getAs[Int](1)).sum

    if (totalDistance == 0) 0
    else nodes * 1.0 / totalDistance
  })
}

