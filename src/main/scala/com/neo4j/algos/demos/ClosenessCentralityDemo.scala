package com.neo4j.algos.demos

import com.neo4j.algos.GoTGraphApp
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.graphframes.GraphFrame
import org.graphframes.lib.AggregateMessages

object ClosenessCentralityDemo extends GoTGraphApp(Some(1)) {

  val vertices = g.vertices.withColumn("ids", array())
  val cachedVertices = AggregateMessages.getCachedDataFrame(vertices)
  val g2 = GraphFrame(cachedVertices, g.edges)

//  (0 to g2.vertices.count) {
//    val msgDst = newPathsUdf(AggregateMessages.src("ids"), AggregateMessages.src("id"))
//    ???
//  }
//
//  val pathsType = ArrayType(
//    StructType(Seq(
//      StructField("id", StringType),
//      StructField("distance", IntegerType)
//    ))
//  )
//
//  val newPathsUdf: UserDefinedFunction = udf((paths: Column, id: Column) => {
//    paths
//  })

}

