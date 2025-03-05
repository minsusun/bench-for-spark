package com.omnia.spark.benchmarks.tests

import com.omnia.spark.benchmarks.graphLoader.ParquetGraphLoader
import com.omnia.spark.benchmarks.{LogTrait, ParseOptions, SQLTest}
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.AuxGraphLoader

class ParquetGraphLoadTest(val options: ParseOptions, spark:SparkSession) extends SQLTest(spark) with LogTrait{

  override def execute(): String = {
    val loaderName = options.getGraphLoader
    val filePath: String = options.getInputFiles()(0)
    if (loaderName.compareToIgnoreCase("graphX") == 0 || loaderName.compareToIgnoreCase("aux") == 0) {
      val loader = new AuxGraphLoader()
      val graph = loader.edgeListFile(spark.sparkContext, filePath)
      concatLog(loader.logToString)
      forceUpdate()
      val _ = graph.cache()
      step("[AuxGraphLoader]Graph Cache")
    } else if (loaderName.compareToIgnoreCase("parquet") == 0) {
      assert(filePath.endsWith(".parquet"), "😡 Given file is not parquet format")
      val loader = new ParquetGraphLoader()
      val graph = loader.load(spark, filePath)
      concatLog(loader.logToString)
      forceUpdate()
      val _ = graph.cache()
      step("[ParquetGraphLoader]Graph Cache");
    } else {
      throw new IllegalArgumentException(s"Wrong Graph Loader Name Given ${loaderName}")
    }
    s"Ran Parquet GraphLoadTest(Loader: ${loaderName}" + logToString
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String = "ParquetGraphLoadTest"
}
