package com.lorelib.spark.java.sparksql;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

/**
 * @author listening
 * @description SparkSQLDataManually:
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

public class SparkSQLDataManually {
  static String[] channels = new String[] {"Spark", "Hadoop", "Flink", "Kafka", "Hive", "Storm", "ML"};
  static String[] actions = new String[] {"View", "Registration"};

  public static void main(String[] args) {
    long numberLogInfo = 5000;
    String path = ".";
    if (args.length > 0) {
      numberLogInfo = Long.valueOf(args[0]);
      if (args.length > 1) path = args[1];
    }

    for (int i = 2; i > 0; i--) {
      String logs = genLogs(i, numberLogInfo);
      saveAsFile(path + "userLogs-" + formatDate(genDate(-i)) + ".log", logs);
    }
  }

  private static void saveAsFile(String path, String logInfo) {
    try (FileOutputStream fos = new FileOutputStream(path)) {
      fos.write(logInfo.getBytes());
      fos.flush();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static String genLogs(int backDay, long numberLogInfo) {
    StringBuffer logs = new StringBuffer();

    Random random = new Random();
    for (int i = 0; i < numberLogInfo; i++) {
      Date date = genDate(-backDay);
      long timestamp = date.getTime();
      long userID = random.nextInt((int) numberLogInfo);
      long pageID = random.nextInt(1000);
      String channel = channels[random.nextInt(channels.length)];
      String action = actions[random.nextInt(actions.length)];

      logs.append(formatDate(date)).append("\t")
          .append(timestamp).append("\t")
          .append(userID).append("\t")
          .append(pageID).append("\t")
          .append(channel).append("\t")
          .append(action).append("\n");
      //System.out.println(logs.toString());
    }
    return logs.toString();
  }

  private static Date genDate(int backDay) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(new Date());
    calendar.add(Calendar.DATE, backDay);
    return calendar.getTime();
  }

  private static String formatDate(Date date) {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    return dateFormat.format(date);
  }
}
