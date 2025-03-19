package com.omnia.spark.benchmarks.graphloader

import com.omnia.spark.benchmarks.ParseOptions
import org.apache.spark.graphx.{AuxGraphLoader, Graph}
import org.apache.spark.sql.SparkSession

class TestGraphLoader(val options: ParseOptions, spark: SparkSession) {
  val loaderName: String = options.getGraphLoader
  private var logString = ""

  def explain: String = logString

  def load(): Graph[Int, Int] = {
    val loaderName = options.getGraphLoader
    val filePath: String = options.getInputFiles()(0)
    if (loaderName.compareToIgnoreCase("graphX") == 0) {
      if (filePath.endsWith(".parquet")) {
        throw new IllegalArgumentException(
          "😡 Input file for GraphX(Aux) Graph Loader should not be in parquet format"
        )
      }
      val loader = new GraphXGraphLoader()

      val graph = loader.load(spark.sparkContext, filePath)
      loader.step("[GraphXGraphLoader]Load Graph")

      val cachedGraph = graph.cache()
      loader.step("[GraphXGraphLoader]Graph Cache")

      logString = loader.logToString

      cachedGraph
    } else if (loaderName.compareToIgnoreCase("aux") == 0) {
      if (filePath.endsWith(".parquet")) {
        throw new IllegalArgumentException(
          "😡 Input file for GraphX(Aux) Graph Loader should not be in parquet format"
        )
      }
      val loader = new AuxGraphLoader()

      val graph = loader.edgeListFile(spark.sparkContext, filePath)
      loader.step("[AuxGraphLoader]Build Graph From Edge Partitions")

      val cachedGraph = graph.cache()
      loader.step("[AuxGraphLoader]Graph Cache")

      logString = loader.logToString

      cachedGraph
    } else if (loaderName.compareToIgnoreCase("parquet") == 0) {
      assert(filePath.endsWith(".parquet"), "😡 Given file is not parquet format")

      val loader = new ParquetGraphLoader()

      val graph = loader.load(spark, filePath)
      loader.step("[ParquetGraphLoader]Reconstruct Graph From Existing RDDs")

      val cachedGraph = graph.cache()
      loader.step("[ParquetGraphLoader]Graph Cache")

      logString = loader.logToString

      cachedGraph
    } else {
      throw new IllegalArgumentException("Wrong Graph Loader Name Given " + loaderName)
    }
  }
}
