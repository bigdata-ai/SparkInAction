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
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author listening
 * @description WordCountOnline:
 * flume-ng agent -n agent1 -c conf -f /usr/local/flume/flume-1.8.0/conf/flume-conf.properties -Dflume.root.logger=DEBUG,console
 * @create 2018 02 21 上午3:03.
 */

public class SparkStreamingPollingDataFromFlume {
  public static void main(String[] args) throws InterruptedException {
    SparkConf conf = new SparkConf().setAppName("SparkStreamingPollingDataFromFlume");
    JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
    JavaReceiverInputDStream lines = FlumeUtils.createPollingStream(jsc, "Master", 9999);
    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<SparkFlumeEvent, String>() {
      @Override
      public Iterable<String> call(SparkFlumeEvent event) throws Exception {
        String line = new String(event.event().getBody().array());
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
