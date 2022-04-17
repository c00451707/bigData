/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object SparkTest1 {
  def main(args: Array[String]): Unit = {
    var appName = "InvertedIndexDemo"
    var master = "local[*]"
    var path = "/Users/fuwuchen/code/bigdata/geektime-bigdata/week6/week6-homework/src/main/resources/input"
    if (args.length > 2) {
      appName = args(0)
      master = args(1)
      path = args(2)
    }
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)

    // for test, large files may cause bad performance
    val filesRDD: RDD[(String, String)] = sc.wholeTextFiles(path)

    val contentToWordOne = (item: (String, String)) => {
      val absPath = item._1
      val filename = absPath.substring(absPath.lastIndexOf("/") + 1)
      ((item._2, filename), 1)
    }

    val aggregateIndex: (mutable.Map[String, Int], (String, Int)) => mutable.Map[String, Int] =
      (array, item) => array += item

    val combineIndex: (mutable.Map[String, Int], mutable.Map[String, Int]) => mutable.Map[String, Int] =
      (array1, array2) => array1 ++ array2

    val printResult = (tuple: (String, mutable.Map[String, Int])) => {
      val word = tuple._1
      val frequencyWithIndex = tuple._2
      println((word, frequencyWithIndex.toArray.mkString("{", ", ", "}")))
    }

    filesRDD
      // word count
      .flatMapValues(item => item.split("\n").flatMap(s => s.split("\\W+")))
      .map(contentToWordOne)
      // pay attention to skew due to large files
      .reduceByKey(_ + _)
      .cache()
      .map(item => (item._1._1, (item._1._2, item._2)))
      // pay attention to skew due to file nums
      .aggregateByKey(mutable.Map[String, Int]())(seqOp = aggregateIndex, combOp = combineIndex)
      .cache()
      // for test
      .collect()
      .sortBy(item => item._1)
      .foreach(printResult)
  }
}
