package com.lorelib.spark.sparksql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @description SparkSQLUDFUDAF: 
  * @author listening
  * @create 2018 02 16 下午1:08.
  */

object SparkSQLUDFUDAF {
  def main(args: Array[String]): Unit = {
    // local[4] 设置local模式下开启4个线程
    val conf = new SparkConf().setMaster("local[4]").setAppName("SparkSQLWindowFunction")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val bigData = Array("Spark", "Hadoop", "Spark", "Spark", "Hadoop", "Spark", "Hadoop", "Spark")
    val bigDataRDD = sc.parallelize(bigData)
    val bigDataRDDRow = bigDataRDD.map(item => Row(item))

    val structType = StructType(Array(StructField("word", StringType, true)))
    val bigDataDF = sqlContext.createDataFrame(bigDataRDDRow, structType)
    bigDataDF.registerTempTable("bigDataTable")

    // 通过sqlContext注册UDF, 在scala2.10.x版本UDF函数最多可以接受22个输入参数
    sqlContext.udf.register("computeLength", (input: String) => input.length)

    //sqlContext.sql("select word, computeLength(word) as length from bigDataTable").show()

    // 注册UDAF
    sqlContext.udf.register("wordCount", new MyUDAF)
    sqlContext.sql("select word, wordCount(word) as count, computeLength(word) as length " +
      "from bigDataTable group by word").show()

    while (true) {}
  }
}

class MyUDAF extends UserDefinedAggregateFunction {
  /**
    * 该方法指定具体输入数据的类型
    * @return
    */
  override def inputSchema: StructType = StructType(Array(StructField("input", StringType, true)))

  /**
    * 进行聚合操作的时候所要处理的数据的结果的类型
    * @return
    */
  override def bufferSchema: StructType = StructType(Array(StructField("count", IntegerType, true)))

  /**
    * 指定UDAF函数计算后返回的结果类型
    * @return
    */
  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  /**
    * 在Aggregate之前每组数据的初始化结果
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = { buffer(0) = 0 }

  /**
    * 在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
    * 本地的聚合操作
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }

  /**
    * 最后再分布式节点进行local reduce完成后需要进行全局级别的merge操作
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }

  /**
    * 返回UDAF最后的计算结果
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)
}
