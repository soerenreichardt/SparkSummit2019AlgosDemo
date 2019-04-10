package com.neo4j.algos.utils

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object GoTDataReader {

  def readNodeData(maybeBookNr: Option[Int])(implicit spark: SparkSession): DataFrame = readData(maybeBookNr, "nodes", nodeSchema)

  def readEdgeData(maybeBookNr: Option[Int])(implicit spark: SparkSession): DataFrame = readData(maybeBookNr, "edges", edgeSchema)

  def nodeSchema: StructType = StructType(Seq(
    StructField("id", StringType, true),
    StructField("name", StringType, true)
  ))

  def edgeSchema: StructType = StructType(Seq(
    StructField("src", StringType, true),
    StructField("dst", StringType, true),
    StructField("type", StringType, true),
    StructField("weight", IntegerType, true),
    StructField("book", IntegerType, true)
  ))

  private def readData(bookNr: Option[Int], entity: String, schema: StructType)(implicit spark: SparkSession): DataFrame = {
    val dfReader = spark.read.schema(schema)
    if (bookNr.isDefined) {
      dfReader.csv(getClass.getResource(s"/asoiaf-book${bookNr.get}-$entity.csv").getFile)
    } else {
      dfReader.csv(getClass.getResource(s"asoiaf-all-$entity.csv").getFile)
    }
  }

}
