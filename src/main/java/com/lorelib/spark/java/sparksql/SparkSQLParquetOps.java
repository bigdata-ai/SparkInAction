package com.lorelib.spark.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.util.List;

/**
 * @author listening
 * @description
 * @date 2018-02-08 12:37
 * @since 1.0
 */
public class SparkSQLParquetOps {
  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "D:\\Develop\\winutils\\hadoop-2.6.0");

    SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkSQLParquetOps");
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc);

    DataFrame usersDF = sqlContext.read().parquet("users.parquet");
    usersDF.registerTempTable("users");

    DataFrame result = sqlContext.sql("select * from users");

    List<Row> listRow = result.javaRDD().collect();
    for (Row row: listRow) {
      System.out.println(row);
    }
  }
}
