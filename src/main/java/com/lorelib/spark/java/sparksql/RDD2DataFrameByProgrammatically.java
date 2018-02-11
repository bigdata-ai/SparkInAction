package com.lorelib.spark.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
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
public class RDD2DataFrameByProgrammatically {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameByProgrammatically");
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc);

    JavaRDD<String> lines = sc.textFile("persons.txt");
    
    JavaRDD<Row> personsRDD = lines.map(new Function<String, Row>() {
      @Override
      public Row call(String line) throws Exception {
        String[] split = line.split(",");
        return RowFactory.create(Integer.valueOf(split[0]), split[1], Integer.valueOf(split[2]));
      }
    });

    List<StructField> structFields = new ArrayList<>();
    structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
    structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
    structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
    StructType structType = DataTypes.createStructType(structFields);
    DataFrame df = sqlContext.createDataFrame(personsRDD, structType);
    df.registerTempTable("persons");
    DataFrame result = sqlContext.sql("select * from persons where age > 8");
    result.show();

    List<Row> listRow = result.javaRDD().collect();
    for (Row row: listRow) {
      System.out.println(row);
    }
  }
}
