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

abstract class Action

case class Count() extends Action {
  override def toString: String = "count"
}

case class Save(fileName: String) extends Action {
  override def toString: String = "save at " + fileName
}

case class Collect(items: Int) extends Action {
  override def toString: String = "collect(" + items + ")"
}

case class Noop() extends Action {
  override def toString: String = "No-Op (no explicit action was necessary)"
}
