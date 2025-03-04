package com.omnia.spark.benchmarks.tests

import com.omnia.spark.benchmarks.{LogTrait, ParseOptions, SQLTest}
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.{AuxGraphLoader, Edge, EdgeRDD, Graph, GraphLoader, VertexRDD}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ParquetConversion (val options: ParseOptions, spark:SparkSession) extends SQLTest(spark) with LogTrait {

  override def execute(): String = {
    var graph: Graph[Int, Int] = null
    if (options.getAuxGraphLoader) {
      val loader = new AuxGraphLoader()

      graph = loader.edgeListFile(spark.sparkContext, options.getInputFiles()(0))
      loader.step("[AuxGraphLoader]Build Graph From Edge Partitions")

      graph = graph.cache()
      loader.step("[AuxGraphLoader]Graph Cache")

      concatLog(loader.logToString)
      forceUpdate();
    } else {
      graph = GraphLoader.edgeListFile(spark.sparkContext, options.getInputFiles()(0))
      step("[GraphX]Load Graph")

      graph = graph.cache()
      step("[GraphX]Graph Cache")
    }

    val edgeDF = spark.createDataFrame(graph.edges)
    step("[Edge]RDD->DF")

    val edgeParquetName = s"${options.getInputFiles()(0)}.edge.parquet"
    edgeDF.write.mode(SaveMode.Overwrite).parquet(edgeParquetName)
    step("[Edge]Saving Parquet")

    "Ran Parquet Conversion on" + options.getInputFiles()(0) + logToString
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String = "ParquetConversion on " + options.getInputFiles()(0)

  override def printAdditionalInformation():String = {
    val sb = new StringBuilder()
    sb.mkString
  }
}
