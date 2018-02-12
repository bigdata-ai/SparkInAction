package com.lorelib.spark.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{RowFactory, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * @description SparkSQLWithJoinOps: 
  * @author listening
  * @create 2018 02 12 上午9:54.
  */

object SparkSQLWithJoinOps {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkSQLWithJoinOps")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val peopleScoresDF = sqlContext.read.json("people2.json")
    peopleScoresDF.registerTempTable("peopleScores")

    val excellentPeoples = sqlContext.sql("select name, score from peopleScores where score > 80")

    val peopleInfosDF = sqlContext.read.json("people.json")
    peopleInfosDF.registerTempTable("peopleInfos")

    val names = excellentPeoples.map(row => row.getAs[String]("name")).collect()
    var sql = "select name, age from peopleInfos where name in ("
    for (idx <- 0 until names.length) {
      sql += "'" + names(idx) + "'"
      if (idx < names.length - 1) {
        sql += ","
      }
    }
    sql += ")"
    println("sql: " + sql)

    val excellentPeoplesInfo = sqlContext.sql(sql)

    val peopleScoresPair = excellentPeoplesInfo.rdd.map(row => (row.getAs[String]("name"), row.getAs[Integer]("age")))
    val peopleInfosPair = excellentPeoples.rdd.map(row => (row.getAs[String]("name"), row.getAs[Integer]("score")))
    val result = peopleScoresPair.join(peopleInfosPair)

    val resultRDD = result.map(tp => RowFactory.create(tp._1, tp._2._1, tp._2._2))

    val structFields = ArrayBuffer.empty[StructField]
    structFields += StructField("name", DataTypes.StringType, true)
    structFields += StructField("age", DataTypes.LongType, true)
    structFields += StructField("score", DataTypes.LongType, true)
    val structType = DataTypes.createStructType(structFields.toArray)
    val df =sqlContext.createDataFrame(resultRDD, structType)
    df.show()
    df.write.json("peopleExcellentResult")
  }
}
