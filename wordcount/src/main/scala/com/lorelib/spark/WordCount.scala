package com.lorelib.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by listening on 2017/3/15.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Word Count!")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val file = sc.textFile("wordcount/README.md", 2)

    val words = file.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.foreach((wordCount => println(wordCount._1 + " : " + wordCount._2)))

    println("-------------------------------------------")

    val sorted = wordCounts.sortBy(_._2, false).take(10)
    sorted.foreach((wordCount => println(wordCount._1 + " : " + wordCount._2)))

    sc.stop()
  }
}
