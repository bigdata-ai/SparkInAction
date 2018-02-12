package com.lorelib.spark.java.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author listening
 * @description SparkSQLWithJoin:
 * @create 2018 02 11 下午7:56.
 */

public class SparkSQLWithJoin {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkSQLWithJoin");
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc);

    DataFrame peopleDF = sqlContext.read().json("people2.json");
    peopleDF.registerTempTable("peopleScores");

    DataFrame excellentScoresDF = sqlContext.sql("select name, score from peopleScores where score > 80");
    List<String> excellentScoresList = excellentScoresDF.javaRDD().map(new Function<Row, String>() {
      @Override
      public String call(Row row) throws Exception {
        return row.getAs("name");
      }
    }).collect();

    List<String> peopleInfos = new ArrayList<>();
    peopleInfos.add("{\"name\":\"Michael\", \"age\": 20}");
    peopleInfos.add("{\"name\":\"Andy\", \"age\":17}");
    peopleInfos.add("{\"name\":\"Justin\", \"age\":19}");

    JavaRDD<String> peopleInfosRDD = sc.parallelize(peopleInfos);
    DataFrame peopleInfosDF = sqlContext.read().json(peopleInfosRDD);
    peopleInfosDF.registerTempTable("peopleInfos");

    String sql = "select name, age from peopleInfos where name in (";
    for (int i = 0; i < excellentScoresList.size(); i++) {
      sql += "'" + excellentScoresList.get(i) + "'";
      if (i < excellentScoresList.size() - 1) {
        sql += ",";
      }
    }
    sql += ")";

    DataFrame nameAgeDF = sqlContext.sql(sql);

    JavaPairRDD<String, Integer> pairRDD = excellentScoresDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(Row row) throws Exception {
        return new Tuple2<String, Integer>(row.getAs("name"), row.getAs("score"));
      }
    });

    JavaPairRDD<String, Tuple2<Integer, Integer>> resultRDD = pairRDD.join(nameAgeDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(Row row) throws Exception {
        return new Tuple2<String, Integer>(row.getAs("name"), row.getAs("age"));
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
    structFields.add(DataTypes.createStructField("age", DataTypes.LongType, true));
    structFields.add(DataTypes.createStructField("score", DataTypes.LongType, true));
    StructType structType = DataTypes.createStructType(structFields);

    DataFrame personDF = sqlContext.createDataFrame(resultRowRDD, structType);
    personDF.show();

    personDF.write().mode(SaveMode.Append).format("json").save("peopleResult.json");
  }
}
