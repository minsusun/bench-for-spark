package com.omnia.spark.benchmarks.tests

import com.omnia.spark.benchmarks.{LogTrait, ParseOptions, SQLTest}
import org.apache.spark.sql.SparkSession

class PageRank (val options: ParseOptions, spark: SparkSession) extends SQLTest(spark) with LogTrait {

  override def execute(): String = ???

  override def explain(): Unit = ???

  override def plainExplain(): String = ???
}
