package com.lorelib.spark.java.sparkstreaming;

import com.google.common.base.Optional;
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
import java.util.List;

/**
 * @author listening
 * @description WordCountOnline:
 * @create 2018 02 21 上午3:03.
 */

public class UpdateStateByKeyDemo {
  public static void main(String[] args) throws InterruptedException {
    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyDemo");
    try (JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));) {
      jsc.checkpoint("data/checkpoint");
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
      JavaPairDStream<String, Integer> wordsCount = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
        @Override
        public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
          Integer updatedValue = 0;

          if (state.isPresent()) {
            updatedValue = state.get();
          }

          for (Integer value: values) {
            updatedValue += value;
          }
          return Optional.of(updatedValue);
        }
      });
      wordsCount.print();

      jsc.start();

      jsc.awaitTermination();

      jsc.close();
    }
  }
}
