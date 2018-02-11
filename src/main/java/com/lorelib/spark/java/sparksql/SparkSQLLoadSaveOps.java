package com.lorelib.spark.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author listening
 * @description
 * @date 2018-02-08 12:37
 * @since 1.0
 */
public class SparkSQLLoadSaveOps {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkSQLLoadSaveOps");
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc);

    DataFrame peopleDF = sqlContext.read().format("json").load("people.json");
    peopleDF.select("name").show();
    peopleDF.select("name").write().mode(SaveMode.Append).save("peopleNames.json");
  }
}
