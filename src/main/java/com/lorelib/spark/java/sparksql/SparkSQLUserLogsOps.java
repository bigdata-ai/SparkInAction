package com.lorelib.spark.java.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author listening
 * @description SparkSQLUserLogsOps:
 * 计算uv:
 * sqlContext.sql("select date_,pageID,uv from (select date_,pageID,count(1) uv from (select date_,pageID,userID FROM userlogs where date_='2018-02-18' group by date_,pageID,userID) b1 group by date_,pageID) b2 order by uv desc").repartition(1000).show(10)
 * @create 2018 02 18 下午10:10.
 */

public class SparkSQLUserLogsOps {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("SparkSQLUserLogsOps");
    JavaSparkContext sc = new JavaSparkContext(conf);
    HiveContext hiveContext = new HiveContext(sc);

    Result[] rets = new Result[2];
    for (int i = 0; i < rets.length; i++) {
      rets[i] = pvStatistic(hiveContext, genDate(-i));
    }
    for(int i = 0; i < rets.length; i++) {
      System.out.println("sql: " + rets[i].sql);
      Row[] result = rets[i].result;
      for (int j = 0; j < result.length; j++) {
        System.out.println(result[j].toString());
      }
    }
  }

  private static Result pvStatistic(HiveContext hiveContext, Date date) {
    String sql = "select date_, pageID, pv from " +
        "(select date_, pageID, count(1) pv from userlogs " +
        " where action='View' and date_='" + formatDate(date) + "' group by date_, pageID) sub " +
        "order by pv asc limit 10";
    DataFrame df = hiveContext.sql(sql);
    //result.show();
    Row[] result = df.collect();
    return new Result(sql, result);
  }

  private static Date genDate(int backDay) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(new Date());
    calendar.add(Calendar.DAY_OF_MONTH, backDay);
    return calendar.getTime();
  }

  private static String formatDate(Date date) {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    return dateFormat.format(date);
  }

  static class Result {
    String sql;
    Row[] result;

    public Result(String sql, Row[] result) {
      this.sql = sql;
      this.result = result;
    }

    public String getSql() {
      return sql;
    }

    public void setSql(String sql) {
      this.sql = sql;
    }

    public Row[] getResult() {
      return result;
    }

    public void setResult(Row[] result) {
      this.result = result;
    }
  }
}
