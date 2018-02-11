package com.lorelib.spark.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.codehaus.janino.Java;

import java.io.Serializable;
import java.util.List;

/**
 * @author listening
 * @description
 * @date 2018-02-07 20:08
 * @since 1.0
 */
public class RDD2DataFrameByReflection {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameByReflection");
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc);

    JavaRDD<String> lines = sc.textFile("persons.txt");
    JavaRDD<Person> persons = lines.map(new Function<String, Person>() {
      @Override
      public Person call(String line) throws Exception {
        String[] split = line.split(",");
        Person p = new Person();
        p.setId(Integer.valueOf(split[0].trim()));
        p.setName(split[1].trim());
        p.setAge(Integer.valueOf(split[2].trim()));
        return p;
      }
    });

    DataFrame df = sqlContext.createDataFrame(persons, Person.class);
    df.registerTempTable("persons");
    DataFrame bigDatas = sqlContext.sql("select * from persons where age >= 6");
    bigDatas.show();

    JavaRDD<Row> bigDataRDD = bigDatas.javaRDD();
    JavaRDD<Person> result = bigDataRDD.map(new Function<Row, Person>() {
      @Override
      public Person call(Row row) throws Exception {
        Person p = new Person();
        p.setId(row.getInt(1));
        p.setName(row.getString(2));
        p.setAge(row.getInt(0));
        return p;
      }
    });

    List<Person> personList = result.collect();
    for (Person person: personList) {
      System.out.println(person);
    }
  }
}
