package com.lorelib.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @description RDDBasedOnCollections: 
  * @author listening
  * @create 2018 01 06 下午6:34.
  */

object RDDBasedOnCollections {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("RDDBasedOnCollections")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val numbers = 1 to 100
    val rdd = sc.parallelize(numbers)

    val sum = rdd.reduce(_+_)

    println(sum)

    sc.stop()
  }
}
