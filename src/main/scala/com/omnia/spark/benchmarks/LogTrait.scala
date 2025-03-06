package com.omnia.spark.benchmarks

trait LogTrait {
  private val indentSize = 2;
  private var lastStepTime = System.nanoTime();
  private var logString = ""

  def step(StepName: String): Unit = {
    val now = System.nanoTime()
    logString += "\n" + "\t" * indentSize + "Step '" + StepName + s"': ${(now - lastStepTime)/1000000} ms"
    lastStepTime = now
  }

  def log(text: String): Unit = {
    logString += "\n" + "\t" * indentSize + text
  }

  def concatLog(externalLogString: String): Unit = {
    logString += externalLogString
  }

  def forceUpdate(): Unit = {
    lastStepTime = System.nanoTime()
  }

  def logToString: String = logString
}
