package com.lorelib.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @description OnlineBlackListFilter: 
  * @author listening
  * @create 2018 02 28 下午8:53.
  */

object OnlineBlackListFilter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("OnlineBlackListFilter")

    val ssc = new StreamingContext(conf, Seconds(60))

    val blackList = Array(("hadoop", true), ("mahout", true))
    val blackListRDD = ssc.sparkContext.parallelize(blackList, 8)

    val adsClickStream = ssc.socketTextStream("localhost", 9999)

    /**
      * 此处模拟的广告点击的每条数据格式为：time、 name
      * map的结果是： name、(time, name)格式
      */
    val adsClickStreamFormatted = adsClickStream.map(ads => (ads.split(" ")(1), ads))
    adsClickStreamFormatted.transform(userClickRDD => {
      val joinedBlackListRDD = userClickRDD.leftOuterJoin(blackListRDD)
      val validClicked = joinedBlackListRDD.filter(joinedItem => {
        println(joinedItem)
        if (joinedItem._2._2.getOrElse(false)) {
          false
        } else {
          true
        }
      })
      validClicked.map(clicked => clicked._2._1)
    }).print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
