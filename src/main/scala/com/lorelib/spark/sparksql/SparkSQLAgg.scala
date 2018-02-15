package com.lorelib.spark.sparksql

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * @description SparkSQLAgg: 
  * @author listening
  * @create 2018 02 15 下午2:00.
  */

object SparkSQLAgg {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkSQLAgg")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val userData = Array(
      "2016-3-27,001,http://spark.apache.org,1000",
      "2016-3-27,001,http://hadoop.apache.org,1001",
      "2016-3-27,002,http://flink.apache.org,1002",
      "2016-3-28,003,http://kafka.apache.org,1003",
      "2016-3-28,004,http://spark.apache.org,1010",
      "2016-3-28,002,http://hive.apache.org,1200",
      "2016-3-28,001,http://parquet.apache.org,1500",
      "2016-3-28,001,http://spark.apache.org,1800"
    )

    val userDataRDD = sc.parallelize(userData)

    val userDataRDDRow = userDataRDD.map(row => {
      val splited = row.split(",")
      Row(splited(0), splited(1).toInt, splited(2), splited(3).toInt)
    })
    val structTypes = StructType(Array(
      StructField("time", StringType, true),
      StructField("id", IntegerType, true),
      StructField("url", StringType, true),
      StructField("amount", IntegerType, true)
    ))

    val userDataDF = sqlContext.createDataFrame(userDataRDDRow, structTypes)

    //要使用Spark SQL的内置函数就一定要导入SQLContext下的隐式转换
    import sqlContext.implicits._

    userDataDF.groupBy("time").agg('time, countDistinct('id)).show()

    userDataDF.groupBy("time").agg('time, sum('amount))
      .map(row => Row(row.get(1), row.get(2))).collect.foreach(println)
  }
}
