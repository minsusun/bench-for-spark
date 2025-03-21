/*
 * Crail SQL Benchmarks
 *
 * Author: Animesh Trivedi <atr@zurich.ibm.com>
 *
 * Copyright (C) 2017, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.omnia.spark.benchmarks

import com.omnia.spark.benchmarks.tests.tpcds.{SingleTPCDSTest, TPCDSTest}
import com.omnia.spark.benchmarks.tests.{
  ConnectedComponents,
  EquiJoin,
  LBFGS,
  NaivePageRank,
  PageRank,
  ParquetConversion,
  ParquetGraphLoadTest,
  ReadOnly,
  SVDPlusPlus
}
import org.apache.spark.sql.SparkSession

object SQLTestFactory {
  def newTestInstance(
    options: ParseOptions,
    spark: SparkSession,
    warnings: StringBuilder
  ): SQLTest = {
    if (options.isTestEquiJoin) {
      new EquiJoin(options, spark)
    } else if (options.isTestQuery) {
      new SingleTPCDSTest(options, spark)
    } else if (options.isTestTPCDS) {
      new TPCDSTest(options, spark)
    } else if (options.isTestReadOnly) {
      new ReadOnly(options, spark)
    } else if (options.isTestPageRank) {
      if (options.getNaiveImplementation) {
        new NaivePageRank(options, spark)
      } else {
        new PageRank(options, spark)
      }
    } else if (options.isTestConnectedComponents) {
      new ConnectedComponents(options, spark)
    } else if (options.isParquetConversion) {
      new ParquetConversion(options, spark)
    } else if (options.isParquetGraphLoadTest) {
      new ParquetGraphLoadTest(options, spark)
    } else if (options.isSVDPlusPlus) {
      new SVDPlusPlus(options, spark)
    } else if (options.isLBFGS) {
      new LBFGS(options, spark)
    } else {
      throw new Exception("Illegal test name ")
    }
  }
}
