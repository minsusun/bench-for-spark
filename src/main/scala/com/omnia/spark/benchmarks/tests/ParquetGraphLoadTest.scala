package com.omnia.spark.benchmarks.tests

import com.omnia.spark.benchmarks.graphLoader.TestGraphLoader
import com.omnia.spark.benchmarks.{LogTrait, ParseOptions, SQLTest}
import org.apache.spark.sql.SparkSession

class ParquetGraphLoadTest(val options: ParseOptions, spark:SparkSession) extends SQLTest(spark) with LogTrait{

  override def execute(): String = {
    val loader = new TestGraphLoader(options, spark)
    val _ = loader.load()

    concatLog(loader.explain)

    s"Ran Parquet GraphLoadTest(Loader: ${loader.loaderName})" + logToString
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String = "ParquetGraphLoadTest"
}
