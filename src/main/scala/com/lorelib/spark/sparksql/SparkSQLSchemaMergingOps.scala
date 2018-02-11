package com.lorelib.spark.sparksql

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @description
  * @author listening
  * @date 2018-02-09 17:09
  * @since 1.0
  */
object SparkSQLSchemaMergingOps {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Develop\\winutils\\hadoop-2.6.0")
    val conf = new SparkConf().setMaster("local").setAppName("SparkSQLSchemaMergingOps")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val df1 = sc.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
    df1.write.mode(SaveMode.Append).parquet("data/test_table/key=1")

    val df2 = sc.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple")
    df2.write.mode(SaveMode.Append).parquet("data/test_table/key=2")

    // 读取分区表
    val df3 = sqlContext.read.option("mergeSchema", "true").parquet("data/test_table")
    df3.printSchema()
    df3.show()
  }
}
