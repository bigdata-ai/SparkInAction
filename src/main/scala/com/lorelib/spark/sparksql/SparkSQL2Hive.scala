package com.lorelib.spark.sparksql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @description SparkSQL2Hive: 
  * @author listening
  * @create 2018 02 15 上午7:42.
  */

object SparkSQL2Hive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQL2Hive")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    //hiveContext.sql("use hive")
    hiveContext.sql("DROP TABLE IF EXISTS people")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS people(name STRING, age INT) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'")
    hiveContext.sql("LOAD DATA LOCAL INPATH '/opt/data/people.txt' INTO TABLE people")

    hiveContext.sql("DROP TABLE IF EXISTS peopleScores")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS peopleScores(name STRING, score INT) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'")
    hiveContext.sql("LOAD DATA LOCAL INPATH '/opt/data/peopleScores.txt' INTO TABLE peopleScores")

    val resultDF = hiveContext.sql("select p.name, p.age, ps.score " +
      "from people p join peopleScores ps on p.name = ps.name where ps.score > 90")

    hiveContext.sql("DROP TABLE IF EXISTS peopleinformresult")
    resultDF.write.saveAsTable("peopleinformresult")

    val dataFromHive = hiveContext.table("peopleinformresult")
    dataFromHive.show()
  }
}
