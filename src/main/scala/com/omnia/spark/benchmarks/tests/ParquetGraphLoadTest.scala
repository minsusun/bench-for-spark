package com.omnia.spark.benchmarks.tests

import com.omnia.spark.benchmarks.graphLoader.TestGraphLoader
import com.omnia.spark.benchmarks.{LogTrait, ParseOptions, SQLTest}
import org.apache.spark.sql.SparkSession

class ParquetGraphLoadTest(val options: ParseOptions, spark:SparkSession) extends SQLTest(spark) with LogTrait{

  override def execute(): String = {
    val loader = new TestGraphLoader(options, spark)
    val graph = loader.load()

    concatLog(loader.explain)
    forceUpdate()

    val v = graph.vertices.count()
    step("[ParquetGraphLoadTest]Count Vertices Number")

    val e = graph.edges.count()
    step("[ParquetGraphLoadTest]Count Edges Number")

    log(s"Loaded Graph Info: ${v} vertices / ${e} edges")

    s"Ran Parquet GraphLoadTest(Loader: ${loader.loaderName})" + logToString
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String = "ParquetGraphLoadTest"
}
