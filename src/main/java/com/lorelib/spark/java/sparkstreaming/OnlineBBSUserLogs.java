package com.lorelib.spark.java.sparkstreaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * @author listening
 * @description OnlineBBSUserLogs:
 * @create 2018 02 21 上午3:03.
 */

public class OnlineBBSUserLogs {
  public static void main(String[] args) throws InterruptedException {
    SparkConf conf = new SparkConf().setAppName("OnlineBBSUserLogs");
    try (JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));) {
      Map<String, String> kafkaParams = new HashMap<>();
      kafkaParams.put("metadata.broker.list", "Master:9092,Worker1:9092,Worker2:9092");
      Set<String> topics = new HashSet<>();
      topics.add("UserLogs");

      JavaPairInputDStream lines = KafkaUtils.createDirectStream(
          jsc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics
      );
      //在线PV计算
      onlinePV(lines);
      //在线UV计算
      //onlinePageUV(lines);
      //在线计算注册人数
      //onlineRegistered(lines);
      //在线计算跳出率
      //onlineJumped(lines);
      //在线计算不同channel的PV
      //onlineChannelPV(lines);

      jsc.start();

      jsc.awaitTermination();
    }
  }

  private static void onlineChannelPV(JavaPairInputDStream lines) {
    lines.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
      @Override
      public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
        String[] logs = t._2.split("\t");
        String channel = logs[4];
        return new Tuple2<>(channel, 1L);
      }
    }).reduceByKey(new Function2<Long, Long, Long>() {
      @Override
      public Long call(Long v1, Long v2) throws Exception {
        return v1 + v2;
      }
    }).count().print();
  }

  private static void onlineJumped(JavaPairInputDStream lines) {
    lines.filter(new Function<Tuple2<String, String>, Boolean>() {
      @Override
      public Boolean call(Tuple2<String, String> v1) throws Exception {
        String[] logs = v1._2.split("\t");
        String action = logs[5];
        if ("View".equals(action)) {
          return true;
        } else {
          return false;
        }
      }
    }).mapToPair(new PairFunction<Tuple2<String, String>, Long, Long>() {
      @Override
      public Tuple2<Long, Long> call(Tuple2<String, String> t) throws Exception {
        String[] logs = t._2.split("\t");
        Long userID = Long.valueOf(logs[2] != null ? logs[2] : "-1");
        return new Tuple2<>(userID, 1L);
      }
    }).reduceByKey(new Function2<Long, Long, Long>() {
      @Override
      public Long call(Long v1, Long v2) throws Exception {
        return v1 + v2;
      }
    }).filter(new Function<Tuple2<Long, Long>, Boolean>() {
      @Override
      public Boolean call(Tuple2<Long, Long> v1) throws Exception {
        if (1 == v1._2) {
          return true;
        } else {
          return false;
        }
      }
    }).count().print();
  }

  private static void onlineRegistered(JavaPairInputDStream lines) {
    lines.filter(new Function<Tuple2<String, String>, Boolean>() {
      @Override
      public Boolean call(Tuple2<String, String> v1) throws Exception {
        String[] logs = v1._2.split("\t");
        String action = logs[5];
        if ("Registration".equals(action)) {
          return true;
        } else {
          return false;
        }
      }
    }).count().print();
  }

  private static void onlinePageUV(JavaPairInputDStream lines) {
    lines.filter(new Function<Tuple2<String, String>, Boolean>() {
      @Override
      public Boolean call(Tuple2<String, String> v1) throws Exception {
        String[] logs = v1._2.split("\t");
        String action = logs[5];
        if ("View".equals(action)) {
          return true;
        } else {
          return false;
        }
      }
    }).map(new Function<Tuple2<String,String>, String>() {
      @Override
      public String call(Tuple2<String, String> v1) throws Exception {
        String[] logs = v1._2.split("\t");
        Long userID = Long.valueOf(logs[2] != null ? logs[2] : "-1");
        long pageID = Long.valueOf(logs[3]);
        return pageID + "_" + userID;
      }
    }).transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
      @Override
      public JavaRDD<String> call(JavaRDD<String> v1) throws Exception {
        return v1.distinct();
      }
    }).mapToPair(new PairFunction<String, Long, Long>() {
      @Override
      public Tuple2<Long, Long> call(String s) throws Exception {
        String[] logs = s.split("_");
        Long pageID = Long.valueOf(logs[0]);
        return new Tuple2<>(pageID, 1L);
      }
    }).reduceByKey(new Function2<Long, Long, Long>() {
      @Override
      public Long call(Long v1, Long v2) throws Exception {
        return v1 + v2;
      }
    }).count().print();
  }

  private static void onlinePV(JavaPairInputDStream lines) {
    JavaPairDStream<String, String> logsDStream = lines.filter(new Function<Tuple2<String, String>, Boolean>() {
      @Override
      public Boolean call(Tuple2<String, String> v1) throws Exception {
        String[] logs = v1._2.split("\t");
        String action = logs[5];
        if ("View".equals(action)) {
          return true;
        } else {
          return false;
        }
      }
    });

    JavaPairDStream<Long, Long> pairs = logsDStream.mapToPair(new PairFunction<Tuple2<String, String>, Long, Long>() {
      @Override
      public Tuple2<Long, Long> call(Tuple2<String, String> v) throws Exception {
        String[] logs = v._2.split("\t");
        Long pageId = Long.valueOf(logs[3]);
        return new Tuple2<Long, Long>(pageId, 1L);
      }
    });
    JavaPairDStream<Long, Long> wordsCount = pairs.reduceByKey(new Function2<Long, Long, Long>() {
      @Override
      public Long call(Long v1, Long v2) throws Exception {
        return v1 + v2;
      }
    });
    wordsCount.count().print();
  }
}
