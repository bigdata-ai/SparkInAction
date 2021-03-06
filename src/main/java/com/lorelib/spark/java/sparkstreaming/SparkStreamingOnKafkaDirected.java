package com.lorelib.spark.java.sparkstreaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * @author listening
 * @description SparkStreamingOnKafkaDirected:
 * 启动kafka: nohup ./kafka-server-start.sh ../config/server.properties &
 * 创建topic: ./kafka-topics.sh --create --zookeeper Master:2181,Worker1:2181,Worker2:2181 --replication-factor 3 --partitions 1 --topic SparkStreamingDirected
 * 创建producer: ./kafka-console-producer.sh --broker-list Master:9092,Worker1:9092,Worker2:9092 --topic SparkStreamingDirected
 * 创建consumer: ./kafka-console-consumer.sh --zookeeper Master:2181,Worker1:2181,Worker2:2181 --from-beginning --topic SparkStreamingDirected
 *
 * @create 2018 02 21 上午3:03.
 */

public class SparkStreamingOnKafkaDirected {
  public static void main(String[] args) throws InterruptedException {
    SparkConf conf = new SparkConf().setAppName("SparkStreamingOnKafkaDirected");
    try (JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10))) {
      Map<String, String> kafkaParameters = new HashMap<>();
      kafkaParameters.put("metadata.broker.list", "Master:9092,Worker1:9092,Worker2:9092");

      Set<String> topics = new HashSet<>();
      topics.add("SparkStreamingDirected");

      JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jsc,
          String.class, String.class, StringDecoder.class, StringDecoder.class,
          kafkaParameters, topics);
      JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
        @Override
        public Iterable<String> call(Tuple2<String, String> tuple) throws Exception {
          return Arrays.asList(tuple._2.split(" "));
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
