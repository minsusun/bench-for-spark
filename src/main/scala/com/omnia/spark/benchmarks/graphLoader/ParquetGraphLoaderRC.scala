package com.omnia.spark.benchmarks.graphLoader

import com.omnia.spark.benchmarks.LogTrait
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

class ParquetGraphLoaderRC extends LogTrait{
  def load(spark: SparkSession, path: String): Graph[Int, Int] = {
    val p = spark.read.parquet(path)
    step("[ParquetGraphLoaderRC]Load Parquet File")

    val pRDD = p.rdd
    step("[ParquetGraphLoaderRC]Parquet => RDD[Row]")

    val eRDD = pRDD.map(row => Edge[Int](row.getLong(0), row.getLong(1)))
    step("[ParquetGraphLoaderRC]RDD[Row] => RDD[Edge]")

    Graph.fromEdges(eRDD, 1)
  }
}
