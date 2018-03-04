package com.lorelib.spark.java.sparkstreaming;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * @author listening
 * @description SparkStreamingDataManuallyProducerForKafka:
 * 论坛数据自动生成代码，数据格式如下：
 * date: 日期，格式为yyyy-MM-dd
 * timestamp: 时间戳
 * userID: 用户ID
 * pageID: 页面ID
 * channel: 板块
 * action: 点击和注册
 *
 * 设置args:  10000000 /opt/logs/user_logs/
 * @create 2018 02 18 下午7:37.
 */

public class SparkStreamingDataManuallyProducerForKafka extends Thread {
  /**
   * 论坛频道
   */
  static String[] channels = new String[] {"Spark", "Hadoop", "Flink", "Kafka", "Hive", "Storm", "ML"};
  /**
   * 用户的2种行为模式
   */
  static String[] actions = new String[] {"View", "Registration"};

  //发送给Kafka的数据类别
  private String topic;
  private Producer<Integer, String> producerForKafka;

  private static String dateToday;
  private static Random random;

  public static void main(String[] args) {
    new SparkStreamingDataManuallyProducerForKafka("UserLogs").start();
  }

  public SparkStreamingDataManuallyProducerForKafka(String topic) {
    this.topic = topic;
    dateToday = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    random = new Random();
    Properties conf = new Properties();
    conf.put("metadata.broker.list", "Master:9092,Worker1:9092,Worker2:9092");
    conf.put("serializer.class", "kafka.serializer.StringEncoder");
    producerForKafka = new Producer<Integer, String>(new ProducerConfig(conf));
  }

  @Override
  public void run() {
    int counter = 0;
    while (true) {
      counter++;
      String userLog = userLogs();
      System.out.println("product: " + userLog);
      producerForKafka.send(new KeyedMessage<Integer, String>(topic, userLog));

      if (0 == counter % 500) {
        counter = 0;
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static String userLogs() {
    StringBuffer logs = new StringBuffer();
    long timestamp = System.currentTimeMillis();
    Integer userID = random.nextInt((int) 1000);
    if (userID.intValue() % 5 == 0) {
      userID = null;
    }

    long pageID = random.nextInt(100);
    String channel = channels[random.nextInt(channels.length)];
    String action = actions[random.nextInt(actions.length)];

    logs.append(dateToday).append("\t")
        .append(timestamp).append("\t")
        .append(userID).append("\t")
        .append(pageID).append("\t")
        .append(channel).append("\t")
        .append(action);
    return logs.toString();
  }
}
