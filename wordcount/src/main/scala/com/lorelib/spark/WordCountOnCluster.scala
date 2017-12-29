package com.lorelib.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @description WordCount: 
  * @author listening
  * @create 2017 12 28 下午11:07.
  */

object WordCountOnCluster {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCount")
    //conf.setMaster("spark://master:7077")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("/wordcount")
    //println(lines.toDebugString)
    val words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.collect.foreach(wp => println(wp._1 + " : " + wp._2))
    val sorted = wordCounts.map(w => (w._2, w._1)).sortByKey(false).filter(_._2 != "").take(10).map(w => (w._2, w._1))
    sorted.foreach(s => println(s._1 + " : " + s._2))
    sc.stop()
  }
}
