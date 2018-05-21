package com.lorelib.spark.sparkstreaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @description SparkStreamingFromKafkaFlume2Hive:
  * @author listening
  * @create 2018 02 28 下午8:53.
  */
case class MessageItem(name: String, age: Int)

object SparkStreamingFromKafkaFlume2Hive {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Please input your kafka braoker list and topics to consume")
      System.exit(1);
    }
    val conf = new SparkConf().setMaster("local[4]").setAppName("SparkStreamingFromKafkaFlume2Hive")

    val ssc = new StreamingContext(conf, Seconds(60))

    val Array(brokers, topicList) = args
    val kafkaParams = Map("metadata.broker.list" -> brokers)
    val topics = topicList.split(",").toSet

    val logsDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    logsDStream.map(_._2.split(",")).foreachRDD(rdd => {
      val hiveContext = new HiveContext(rdd.context)

      import hiveContext.implicits._
      rdd.map(record => MessageItem(record(0).trim, record(1).trim.toInt))
        .toDF
        .registerTempTable("temp")
      hiveContext.sql("select count(1) from temp").show()
    })


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
