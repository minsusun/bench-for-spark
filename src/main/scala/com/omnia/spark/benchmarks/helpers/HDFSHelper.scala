package com.omnia.spark.benchmarks.helpers

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object HDFSHelper {
  def HDFSExists(conf: Configuration, path: String): Boolean = FileSystem.get(conf).exists(new Path(path))
  def HDFSExists(conf: Configuration, path: Path): Boolean = FileSystem.get(conf).exists(path)

  def HDFSDeleteIfExists(conf: Configuration, path: String): Unit = {
    val fs = FileSystem.get(conf)
    val hdfsPath = new Path(path)
    if (HDFSExists(conf, hdfsPath)) {
      fs.delete(hdfsPath, true)
    }
  }
}
