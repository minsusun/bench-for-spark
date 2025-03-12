package com.omnia.spark.benchmarks.tests

import com.omnia.spark.benchmarks.{LogTrait, ParseOptions, SQLTest}
import org.apache.spark.graphx.Edge
import org.apache.spark.sql.SparkSession

class SVDPlusPlus(val options: ParseOptions, spark:SparkSession) extends SQLTest(spark) with LogTrait{

  override def execute(): String = {
    val sc = spark.sparkContext

    val textFile = sc.textFile(options.getInputFiles()(0))
    step("[SVD++]Text File Read")

    val edges = textFile.map( line => {
        val lineArray = line.split("\\s+")
        Edge[Double](lineArray(0).toLong * 2, lineArray(1).toLong * 2 + 1, lineArray(2).toDouble)
    })
    step("[SVD++]Build Edge RDD")

    // TODO: extract configuration to parse options
    val conf = new org.apache.spark.graphx.lib.SVDPlusPlus.Conf(10, 5, 0.0, 5.0, 0.007, 0.007, 0.005, 0.015)
    step("[SVD++]Make Configuration")

    val (graph, _) = org.apache.spark.graphx.lib.SVDPlusPlus.run(edges, conf)
    graph.cache()

    val err = graph.vertices.map { case (vid, vd) => if (vid % 2 == 1) vd._4 else 0.0 }.reduce(_ + _) / graph.numEdges
    step("[SVD++]Calculate Error")

    log(s"Error: ${err}")

    s"Ran SVD++ on ${options.getInputFiles()(0)}" + logToString
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String = "SVD++"
}
