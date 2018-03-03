package com.lorelib.spark.sparkstreaming

import com.lorelib.spark.java.commons.ConnectionPool
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @description OnlineTop3ItemForeachCategory:
  *              数据格式： user item category
  *
  *              t1 iphone4 iphone
  *              t2 iphone8 iphone
  *              s5 hwp8 android
  *              t3 iphone8 iphone
  *              s7 hwp8 android
  *              t4 iphone8 iphone
  *              s1 meizu android
  *              s2 xiaomi3 android
  *              t5 iphone8 iphone
  *              t6 iphone8 iphone
  *              s3 xiaomi3 android
  *              t7 iphone7 iphone
  *              s8 hwp8 android
  *              t8 iphone7 iphone
  * @author listening
  * @create 2018 03 02 下午11:12.
  */

object OnlineTop3ItemForeachCategory {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OnlineTop3ItemForeachCategory")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("/opt/checkpoint/OnlineTop3ItemForeachCategory")

    val userClickLogsDStream = ssc.socketTextStream("Master", 9999)

    val formattedUserClickLogsDStream = userClickLogsDStream.map(clickLog =>
      (clickLog.split(" ")(2) + "_" + clickLog.split(" ")(1), 1)
    )
    val categoryUserClickLogsDStream = formattedUserClickLogsDStream.reduceByKeyAndWindow(
      (v1: Int, v2: Int) => v1 + v2, (v1: Int, v2: Int) => v1 - v2, Seconds(60), Seconds(20)
    )

    categoryUserClickLogsDStream.foreachRDD ( rdd => {
      if (rdd.isEmpty) {
        println("No data inputted!!!")
      } else {
        val categoryItemRow = rdd.map(reducedItem => {
          val category = reducedItem._1.split("_")(0)
          val item = reducedItem._1.split("_")(1)
          val clickCount = reducedItem._2
          Row(category, item, clickCount)
        })

        val structType = StructType(Array(
          StructField("category", StringType, true),
          StructField("item", StringType, true),
          StructField("click_count", IntegerType, true)
        ))

        val hiveContext = new HiveContext(rdd.context)
        val categoryItemDF = hiveContext.createDataFrame(categoryItemRow, structType)
        categoryItemDF.registerTempTable("categoryItemTable")

        val resultDF = hiveContext.sql("select category,item,click_count from (" +
          "select category,item,click_count,row_number() over (partition by category order by click_count desc) rank from categoryItemTable" +
          ")sub where rank <=3")
        val resultRowRDD = resultDF.rdd
        resultDF.show()

        resultRowRDD.foreachPartition(partitionOfRecords => {
          if (rdd.isEmpty) {
            println("No data inputted!!!")
          } else {
            val conn = ConnectionPool.getConnection
            partitionOfRecords.foreach(record => {
              val sql = "insert into category_item_top3(category,item,count) values('" + record.getAs("category") + "','" + record.getAs("item") + "'," + record.getAs("click_count") + ");"
              val stmt = conn.createStatement()
              stmt.executeUpdate(sql)
            })
            ConnectionPool.returnConnection(conn)
          }
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
