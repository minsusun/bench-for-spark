package com.omnia.spark.benchmarks.datagenerator

import com.omnia.spark.benchmarks.LogTrait
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class KMeansDataGenerator extends LogTrait {
  def generate(
    spark: SparkSession,
    numPoints: Int,
    dimension: Int,
    k: Int,
    scaling: Double,
    numPartitions: Int
  ): DataFrame = {
    val data = org.apache.spark.mllib.util.KMeansDataGenerator.generateKMeansRDD(
      spark.sparkContext,
      numPoints,
      k,
      dimension,
      scaling,
      numPartitions
    )
    step("[KMeansDataGenerator]Generate KMeans RDD")

    val schemaString = data.first().indices.map(i => "c" + i.toString).mkString(" ")
    val fields =
      schemaString.split(" ").map(fieldName => StructField(fieldName, DoubleType, nullable = false))
    val schema = StructType(fields)
    val rowRDD = data.map(a => Row(a: _*))
    val df = spark.createDataFrame(rowRDD, schema)
    step("[KMeansDataGenerator]Convert to DF")

    df
  }
}
