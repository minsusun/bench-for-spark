package com.omnia.spark.benchmarks.tests

import com.omnia.spark.benchmarks.{LogTrait, ParseOptions, SQLTest}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{LogisticGradient, SquaredL2Updater}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.util.MLUtils

class LBFGSConf(
  val splitRatio: Double,
  val numCorrections: Int,
  val convergenceTol: Double,
  val maxNumIterations: Int,
  val regParam: Double,
  val seed: Long) {
  assert(0 < splitRatio && splitRatio <= 1)
  assert(maxNumIterations > 0)
}

class LBFGS(val options: ParseOptions, spark: SparkSession) extends SQLTest(spark) with LogTrait{

  override def execute(): String = {
    val conf = options.getLBFGSConf

    val data = MLUtils.loadLibSVMFile(spark.sparkContext, options.getInputFiles()(0))
    step("[LBFGS]Read LIBSVM File")

    val numFeatures = data.take(1)(0).features.size
    step("[LBFGS]Find the Number of Features")

    val splits = data.randomSplit(Array(conf.splitRatio, 1 - conf.splitRatio), seed = conf.seed)
    val training = splits(0).map(x => (x.label, MLUtils.appendBias(x.features))).cache()
    val test = splits(1)
    step("[LBFGS]Data Split")

    val initialWeightsWithIntercept = Vectors.dense(new Array[Double](numFeatures + 1))
    step("[LBFGS]Initialize Weights")

    val (weightsWithIntercept, loss) = org.apache.spark.mllib.optimization.LBFGS.runLBFGS(
      training,
      new LogisticGradient(),
      new SquaredL2Updater(),
      conf.numCorrections,
      conf.convergenceTol,
      conf.maxNumIterations,
      conf.regParam,
      initialWeightsWithIntercept
    )
    step("[LBFGS]Execution")

    val model = new LogisticRegressionModel(
      Vectors.dense(weightsWithIntercept.toArray.slice(0, weightsWithIntercept.size - 1)),
      weightsWithIntercept(weightsWithIntercept.size - 1)
    )
    model.clearThreshold()
    step("[LBFGS]Build LogisticRegressionModel from LBFGS results")

    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    step("[LBFGS]Predict, Score and Label")

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    step("[LBFGS]Metrics")

    log("Loss of each step in training")
    loss.zipWithIndex.foreach{ case(l, i) => log(s"step $i: $l")}
    log(s"Area Under ROC = $auROC")
    s""
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String
        = s"L-BFGS(Limited-memory Broyden-Fletcher-Goldfarb-Shanno Algorithm) on ${options.getInputFiles()(0)}"
}
