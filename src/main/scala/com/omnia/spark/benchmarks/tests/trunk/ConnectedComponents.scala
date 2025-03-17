package com.omnia.spark.benchmarks.tests.trunk

import com.omnia.spark.benchmarks.{Action, Noop, ParseOptions, SQLTest}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

class ConnectedComponents(val options: ParseOptions, spark: SparkSession) extends SQLTest(spark) {
  /* we got the input file, we then load it */
  private val start = System.nanoTime()
  private val graph =
    GraphLoader.edgeListFile(spark.sparkContext, options.getInputFiles()(0)).cache()
  private val end = System.nanoTime()

  override def execute(): String = {
    /* we don't have to take any action from the datasets */
    val result =
      org.apache.spark.graphx.lib.ConnectedComponents.run(this.graph, options.getIterations)
    result.vertices.coalesce(1).saveAsTextFile("dbg/resultVE")
    result.edges.coalesce(1).saveAsTextFile("dbg/resultEG")

    "Ran ConnectedComponents " + options.getIterations + " iterations on " + options
      .getInputFiles()(0)
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String =
    "ConnectedComponents " + options.getIterations + " iterations on " + options.getInputFiles()(0)

  override def printAdditionalInformation(): String = {
    val sb = new StringBuilder()
    sb.append("Graph load time: " + (end - start) / 1000000 + " msec\n")
    val str = options.getAction match {
      case noop: Noop => ""
      case a: Action =>
        "Warning: action " + a.toString + " was ignored. PageRank does not need any explicit action.\n"
    }
    sb.append(str)
    sb.mkString
  }
}
