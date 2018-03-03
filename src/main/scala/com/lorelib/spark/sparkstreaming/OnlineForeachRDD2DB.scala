package com.lorelib.spark.sparkstreaming

import com.lorelib.spark.java.commons.ConnectionPool
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @description OnlineForeachRDD2DB: 
  * @author listening
  * @create 2018 03 01 下午1:05.
  */

object OnlineForeachRDD2DB {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("OnlineForeachRDD2DB")

    val ssc = new StreamingContext(conf, Seconds(60))

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)
    wordCounts.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val conn = ConnectionPool.getConnection
        partitionOfRecords.foreach(record => {
          val sql = "insert into streaming_items_count(item,count) values('" + record._1 + "'," + record._2 + ");"
          val stmt = conn.createStatement()
          stmt.executeUpdate(sql)
        })
        ConnectionPool.returnConnection(conn)
      })
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
