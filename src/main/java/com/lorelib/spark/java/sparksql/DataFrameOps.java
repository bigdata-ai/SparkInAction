package com.lorelib.spark.java.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * @author listening
 * @description
 * @date 2018-02-07 13:11
 * @since 1.0
 */
public class DataFrameOps {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("DataFrameOps");
    //conf.setMaster("spark://Master:7077");
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc);
    DataFrame df = sqlContext.read().json("hdfs://Master:9000/library/examples/src/main/resources/people.json");
    df.show();

    df.printSchema();

    df.select("name").show();

    df.select(df.col("name"), df.col("age").plus(10)).show();

    df.filter(df.col("age").gt(10)).show();

    df.groupBy(df.col("age")).count().show();
    sc.close();
  }
}
