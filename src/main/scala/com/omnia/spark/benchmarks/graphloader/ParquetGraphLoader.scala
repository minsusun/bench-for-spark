package com.omnia.spark.benchmarks.graphloader

import com.omnia.spark.benchmarks.LogTrait
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

class ParquetGraphLoader extends LogTrait {
  def load(spark: SparkSession, path: String): Graph[Int, Int] = {
    val p = spark.read.parquet(path)
    step("[ParquetGraphLoader]Load Parquet File")

    val pRDD = p.rdd
    step("[ParquetGraphLoader]Parquet => RDD[Row]")

    val eRDD = pRDD.map(row => Edge[Int](row.getLong(0), row.getLong(1)))
    step("[ParquetGraphLoader]RDD[Row] => RDD[Edge]")

    Graph.fromEdges(eRDD, 1)
  }
}
