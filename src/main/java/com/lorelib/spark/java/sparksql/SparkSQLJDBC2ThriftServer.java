package com.lorelib.spark.java.sparksql;

import java.sql.*;

/**
 * @author listening
 * @description SparkSQLJDBC2ThriftServer:
 * @create 2018 02 17 下午10:29.
 */

public class SparkSQLJDBC2ThriftServer {
  public static void main(String[] args) throws ClassNotFoundException {
    String sql = "select name from people where age = ?";
    Class.forName("org.apache.hive.jdbc.HiveDriver");
    try(Connection conn = DriverManager.getConnection(
        "jdbc:hive2://master:10001/hive?" +
        "hive.server2.transport.mode=http;hive.server2.thrift.http.path=cliservice",
        "root", "123456");
        PreparedStatement statement = conn.prepareStatement(sql);
    ) {
      statement.setInt(1, 30);
      ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        System.out.println(resultSet.getString(1));
      }
      resultSet.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
