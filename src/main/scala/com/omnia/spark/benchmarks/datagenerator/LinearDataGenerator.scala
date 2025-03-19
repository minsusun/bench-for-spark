package com.omnia.spark.benchmarks.datagenerator

import com.omnia.spark.benchmarks.LogTrait
import org.apache.spark.sql.{DataFrame, SparkSession}

class LinearDataGenerator extends LogTrait {
  def generate(
    spark: SparkSession,
    numExamples: Int,
    numFeatures: Int,
    eps: Double,
    numPartitions: Int = 2,
    intercept: Double = 0.0
  ): DataFrame = {
    val data = org.apache.spark.mllib.util.LinearDataGenerator.generateLinearRDD(
      spark.sparkContext,
      numExamples,
      numFeatures,
      eps,
      numPartitions,
      intercept
    )
    step("[LinearDataGenerator]Generate Linear RDD")

    import spark.implicits._
    val df = data.toDF
    step("[LinearDataGenerator]Convert RDD to DF")

    df
  }
}
