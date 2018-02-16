package com.lorelib.spark.sparksql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @description SparkSQLWindowFunction: 
  * @author listening
  * @create 2018 02 15 下午7:17.
  */

object SparkSQLWindowFunction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLWindowFunction")
    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)

    hiveContext.sql("DROP TABLE IF EXISTS scores")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS scores(name STRING, score INT) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\\n'")
    hiveContext.sql("LOAD DATA LOCAL INPATH '/opt/data/topNGroup.txt' INTO TABLE scores")

    val result = hiveContext.sql(
      "SELECT name, score FROM (" +
        "SELECT name, score, row_number() OVER (PARTITION BY name ORDER BY score DESC) rank " +
        "FROM scores" +
      ")sub_scores " +
        "WHERE rank <= 4 ")
    result.show()

    hiveContext.sql("DROP TABLE IF EXISTS sorted_scores")
    result.write.saveAsTable("sorted_scores")
  }
}
