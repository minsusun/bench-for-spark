package com.omnia.spark.benchmarks.tests

import com.omnia.spark.benchmarks.graphLoader.TestGraphLoader
import com.omnia.spark.benchmarks.{LogTrait, ParseOptions, SQLTest}
import org.apache.spark.sql.SparkSession

class PageRank (val options: ParseOptions, spark: SparkSession) extends SQLTest(spark) with LogTrait {

  override def execute(): String = {
    val loader = new TestGraphLoader(options, spark);
    val graph = loader.load()

    concatLog(loader.explain)
    forceUpdate()

    org.apache.spark.graphx.lib.PageRank.run(graph, options.getPageRankIterations)
    step("[PageRank]Execution")

    "Ran PageRank " + options.getPageRankIterations + " iterations on " + options.getInputFiles()(0) + logToString
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String = ""
}
