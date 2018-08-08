/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gridgain

import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.spark.{SparkConf, SparkContext}

object RDDWriter extends App {
  val conf = new SparkConf().setAppName("RDDWriter")
  val sc = new SparkContext(conf)
  val ic = new IgniteContext(sc, "/path_to_ignite_home/examples/config/spark/example-shared-rdd.xml")
  val sharedRDD: IgniteRDD[Int, Int] = ic.fromCache("sharedRDD")
  sharedRDD.savePairs(sc.parallelize(1 to 1000, 10).map(i => (i, i)))
  ic.close(true)
  sc.stop()
}

object RDDReader extends App {
  val conf = new SparkConf().setAppName("RDDReader")
  val sc = new SparkContext(conf)
  val ic = new IgniteContext(sc, "/path_to_ignite_home/examples/config/spark/example-shared-rdd.xml")
  val sharedRDD: IgniteRDD[Int, Int] = ic.fromCache("sharedRDD")
  val greaterThanFiveHundred = sharedRDD.filter(_._2 > 500)
  println("The count is " + greaterThanFiveHundred.count())
  ic.close(true)
  sc.stop()
}