package com.omnia.spark.benchmarks.tests

import com.omnia.spark.benchmarks.graphLoader.TestGraphLoader
import com.omnia.spark.benchmarks.{LogTrait, ParseOptions, SQLTest}
import org.apache.spark.sql.SparkSession

class ConnectedComponents (val options: ParseOptions, spark:SparkSession) extends SQLTest(spark) with LogTrait{

  override def execute(): String = {
    val loader = new TestGraphLoader(options, spark)
    val graph = loader.load()

    concatLog(loader.explain)
    forceUpdate()

    val result = org.apache.spark.graphx.lib.ConnectedComponents.run(graph, options.getPageRankIterations)
    step("[ConnectedComponents]Execution")

    result.vertices.coalesce(1).saveAsTextFile("dbg/resultVE")
    step("[ConnectedComponents]Save resultVE")

    result.edges.coalesce(1).saveAsTextFile("dbg/resultEG")
    step("[ConnectedComponents]Save resultEG")

    "Ran ConnectedComponents " + options.getPageRankIterations + " iterations on " + options.getInputFiles()(0) + logToString
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String = "ConnectedComponents " + options.getPageRankIterations + " iterations on " + options.getInputFiles()(0)
}
