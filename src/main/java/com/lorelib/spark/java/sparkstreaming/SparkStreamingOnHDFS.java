package com.lorelib.spark.java.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author listening
 * @description WordCountOnline:
 * @create 2018 02 21 上午3:03.
 */

public class SparkStreamingOnHDFS {
  public static void main(String[] args) throws InterruptedException {
    final String checkpointDirectory = "hdfs://master:9000/library/SparkStreaming/CheckPoint_Data";

    final SparkConf conf = new SparkConf().setMaster("spark://master:7077").setAppName("SparkStreamingOnHDFS");

    JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
      @Override
      public JavaStreamingContext create() {
        return createContext(conf, checkpointDirectory);
      }
    };
    JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpointDirectory, factory);
    JavaDStream lines = jsc.textFileStream("hdfs://master:9000/library/SparkStreaming/Data");
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

  public static JavaStreamingContext createContext(SparkConf conf, String checkpointDirectory) {
    System.out.println("Creating new context");
    JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(8));
    jsc.checkpoint(checkpointDirectory);
    return jsc;
  }
}
