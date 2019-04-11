package com.neo4j.algos.utils

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object GoTDataReader {

  def readNodeData(maybeNr: Option[Int], subFolder: String)(implicit spark: SparkSession): DataFrame =
    readData(maybeNr, subFolder, "nodes", nodeSchema)

  def readEdgeData(maybeNr: Option[Int], subFolder: String)(implicit spark: SparkSession): DataFrame =
    readData(maybeNr, subFolder, "edges", edgeSchema)

  def nodeSchema: StructType = StructType(Seq(
    StructField("id", StringType, true),
    StructField("name", StringType, true)
  ))

  def edgeSchema: StructType = StructType(Seq(
    StructField("src", StringType, true),
    StructField("dst", StringType, true)
//    StructField("weight", IntegerType, true)
  ))

  private def readData(bookNr: Option[Int], subFolder: String, entity: String, schema: StructType)(implicit spark: SparkSession): DataFrame = {
    val dfReader = spark.read.option("header", "true").schema(schema)
    val rootPath = s"/$subFolder"
    val path = if (bookNr.isDefined) {
      getClass.getResource(s"$rootPath${bookNr.get}-$entity.csv").getFile
    } else {
      getClass.getResource(s"$rootPath-all-$entity.csv").getFile
    }
    println(path)
    dfReader.csv(path)
  }

}
