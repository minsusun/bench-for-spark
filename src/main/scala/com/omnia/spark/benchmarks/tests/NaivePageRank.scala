package com.omnia.spark.benchmarks.tests

import com.omnia.spark.benchmarks.{LogTrait, ParseOptions, SQLTest}
import org.apache.spark.sql.SparkSession


class NaivePageRank(val options: ParseOptions, spark: SparkSession) extends SQLTest(spark) with LogTrait{

  override def execute(): String = {
    val textFile = spark.read.textFile(options.getInputFiles()(0))
    step("[NaivePageRank]Text File Read")

    val linesRDD = textFile.rdd
    step("[NaivePageRank]Text to RDD")

    val links = linesRDD.map(l => {
      val parts = l.split("\\s+")
      (parts(0), parts(1))
    }).distinct().groupByKey().cache()
    step("[NaivePageRank]Build Links RDD")

    var ranks = links.mapValues(_ => 1.0)
    step("[NaivePageRank]Rank Init")


    for (i <- 1 to options.getPageRankIterations) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      step(s"[NaivePageRank]Iteration ${i}: Contribs build")

      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      step(s"[NaivePageRank]Iteration ${i}: Calculate Rank")
    }

    val output = ranks.collect()
    step(s"[NaivePageRank]Rank Collect")

    s"Rank Naive PageRank on ${options.getInputFiles()(0)}" + logToString
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String = "NaivePageRank"
}
