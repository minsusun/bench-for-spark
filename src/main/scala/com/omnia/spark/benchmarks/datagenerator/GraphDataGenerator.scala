package com.omnia.spark.benchmarks.datagenerator

import com.omnia.spark.benchmarks.LogTrait
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

class GraphDataGenerator extends LogTrait {
  def generateLogNormal(
    spark: SparkSession,
    numVertices: Int,
    numPartitions: Int = 0,
    mu: Double = 4.0,
    sigma: Double = 1.3,
    seed: Long = -1
  ): Graph[Long, Int] = {
    val graph = org.apache.spark.graphx.util.GraphGenerators.logNormalGraph(
      spark.sparkContext,
      numVertices,
      numPartitions,
      mu,
      sigma,
      seed
    )
    step("[GraphDataGenerator]Generate Log Normal Graph")

    graph
  }

  def generateRMATGraph(
    spark: SparkSession,
    requestedNumVertices: Int,
    numEdges: Int
  ): Graph[Int, Int] = {
    val graph = org.apache.spark.graphx.util.GraphGenerators
      .rmatGraph(spark.sparkContext, requestedNumVertices, numEdges)
    step("[GraphDataGenerator]Generate R-MAT Graph")

    graph
  }
}
