package com.omnia.spark.benchmarks.tests

import com.omnia.spark.benchmarks.graphloader.TestGraphLoader
import com.omnia.spark.benchmarks.{LogTrait, ParseOptions, SQLTest}
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.PartitionStrategy // ★ 추가된 임포트

class NaivePageRank(val options: ParseOptions, spark: SparkSession)
  extends SQLTest(spark)
    with LogTrait {

  override def execute(): String = {
    val loader = new TestGraphLoader(options, spark)
    // 1. 원본 그래프 로드
    val rawGraph = loader.load()

    concatLog(loader.explain)
    forceUpdate()

    // 2. ★ [핵심] 2D 파티셔닝을 끄고 1D 파티셔닝(Source 기반)으로 강제 재정렬
    val partitionedGraph = rawGraph.partitionBy(PartitionStrategy.EdgePartition1D)
    step("[PageRank] Apply 1D Partitioning")

    // 3. 파티셔닝이 완료된 그래프를 PageRank 알고리즘에 주입
    org.apache.spark.graphx.lib.PageRank.run(partitionedGraph, options.getIterations)
    step("[PageRank] Execution")

    "Ran PageRank (1D Partitioned) " + options.getIterations + " iterations on " + options
      .getInputFiles()(
        0
      ) + logToString
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String =
    s"Page Rank (1D Partitioned) ${options.getIterations} iterations on ${options.getInputFiles()(0)}"
}
