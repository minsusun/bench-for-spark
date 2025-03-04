package com.omnia.spark.benchmarks

trait LogTrait {
  private var lastStepTime = System.nanoTime();
  private var titleString = ""
  private var logString = ""

  def title: String = titleString

  def setTitle(t: String): Unit = {
    titleString = t
  }

  def step(StepName: String): Unit = {
    val now = System.nanoTime()
    logString += "\n\t\t\t\tStep '" + StepName + s"': ${(now - lastStepTime)/1000000} ms"
    lastStepTime = now
  }

  def concatLog(externalLogString: String): Unit = {
    logString += externalLogString
  }

  def forceUpdate(): Unit = {
    lastStepTime = System.nanoTime()
  }

  def logToString: String = logString
  def logToStringWithTitle: String = title + logString
}
