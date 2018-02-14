package com.lorelib.spark.java.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author listening
 * @description SparkSQLJDBC2MySQL:
 * @create 2018 02 13 下午7:09.
 */

public class SparkSQLJDBC2MySQL {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameByReflection");
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc);

    DataFrameReader reader = sqlContext.read().format("jdbc");
    reader.option("url", "jdbc:mysql://127.0.0.1:3306/spark");
    reader.option("dbtable", "user_info");
    reader.option("driver", "com.mysql.jdbc.Driver");
    reader.option("user", "root");
    reader.option("password", "123456");

    DataFrame userInfoDF = reader.load();

    reader.option("dbtable", "user_score");
    DataFrame scoresDF = reader.load();

    JavaPairRDD<String, Integer> pairRDD = userInfoDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(Row row) throws Exception {
        return new Tuple2<String, Integer>(row.getAs("name"), row.getAs("age"));
      }
    });

    JavaPairRDD<String, Tuple2<Integer, Integer>> resultRDD = pairRDD.join(scoresDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(Row row) throws Exception {
        return new Tuple2<String, Integer>(row.getAs("name"), row.getAs("score"));
      }
    }));

    JavaRDD<Row> resultRowRDD = resultRDD.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {
      @Override
      public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
        return RowFactory.create(tuple._1, tuple._2._1, tuple._2._2);
      }
    });

    List<StructField> structFields = new ArrayList<>();
    structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
    structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
    structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
    StructType structType = DataTypes.createStructType(structFields);

    DataFrame personDF = sqlContext.createDataFrame(resultRowRDD, structType);
    personDF.show();

    personDF.javaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
      @Override
      public void call(Iterator<Row> rowIterator) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/spark", "root", "123456");
             Statement statement = conn.createStatement();) {
          String sql = "";
          while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            System.out.println(row);
          }
          //statement.execute(sql);
        }
      }
    });
  }
}
