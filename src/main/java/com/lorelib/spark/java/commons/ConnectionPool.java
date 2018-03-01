package com.lorelib.spark.java.commons;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * @author listening
 * @description ConnectionPool:
 * @create 2018 03 01 下午12:37.
 */

public class ConnectionPool {
  private static LinkedList<Connection> connectionQueque;

  static {
    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public synchronized static Connection getConnection() {
    if (connectionQueque == null) {
      connectionQueque = new LinkedList<>();
      try {
        for (int i = 0; i < 5; i++) {
          Connection conn = DriverManager.getConnection(
              "jdbc:mysql://localhost:3306/spark",
              "root", "123456");
          connectionQueque.push(conn);
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return connectionQueque.poll();
  }

  public static void returnConnection(Connection conn) {
    connectionQueque.push(conn);
  }
}
