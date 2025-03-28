package com.omnia.spark.benchmarks.graphloader

import com.omnia.spark.benchmarks.LogTrait
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, GraphLoader}

class GraphXGraphLoader extends LogTrait {
  def load(sc: SparkContext, path: String): Graph[Int, Int] = GraphLoader.edgeListFile(sc, path)
}
