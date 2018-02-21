package com.lorelib.spark.java.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author listening
 * @description WordCountOnline:
 * @create 2018 02 21 上午3:03.
 */

public class WordCountOnline {
  public static void main(String[] args) throws InterruptedException {
    // 至少有2个线程
    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCountOnline");
    //SparkConf conf = new SparkConf().setMaster("spark://master:7077").setAppName("WordCountOnline");
    try (JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));) {
      JavaReceiverInputDStream lines = jsc.socketTextStream("localhost", 9999);
      JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
        @Override
        public Iterable<String> call(String line) throws Exception {
          return Arrays.asList(line.split(" "));
        }
      });
      JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(String word) throws Exception {
          return new Tuple2<>(word, 1);
        }
      });
      JavaPairDStream<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer v1, Integer v2) throws Exception {
          return v1 + v2;
        }
      });
      wordsCount.print();

      jsc.start();

      jsc.awaitTermination();

      jsc.close();
    }
  }
}
