package com.omnia.spark.benchmarks.helpers

import org.apache.spark.sql.DataFrame

object SparkHelper {
  def writeToDisk(data: DataFrame, outputDir: String): Unit = {
    val format = outputDir.split('.').last
    format match {
      case FileFormats.parquet => data.write.parquet(outputDir)
      case FileFormats.csv     => data.write.option("header", "true").csv(outputDir)
      case FileFormats.json    => data.write.json(outputDir)
      case _                   => throw new Exception(s"Unsupported format to save on disk")
    }
  }
}
