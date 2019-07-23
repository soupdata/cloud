package com.godalgo.inspirer.cloud.analytics.repository;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.sql.*;

/**
 * Created by 杭盖 on 2018/1/3.
 * 使用 org.apache.tomcat.jdbc 代替
 *
 */
/*
@Slf4j
@Repository("PhoenixConnector")
public class PhoenixConnector {

  @Value("${phoenix.url}")
  private static String phoenixUrl;

  public static Connection getConnection() {
    Connection con = null;
    try {
      con = DriverManager.getConnection(phoenixUrl);
    } catch (SQLException e) {
      e.printStackTrace();
      log.error("Connection fail: ", e);
    }

    //dataSource.setDriverClassName("org.apache.phoenix.jdbc.PhoenixDriver");
    log.info("Initialized hbase");

    return con;
  }

  public static ResultSet executeQuery(String sql) throws SQLException {
    ResultSet rs = null;
    Connection conn = getConnection();
    Statement statement = conn.createStatement();
    try {
      rs = statement.executeQuery(sql);
    } catch (Throwable ex) {

      //logger.error(s"$sql - ${ex.toString}")
    }


    // 关闭资源
    statement.close();
    conn.close();
    return rs;
  }

  public static Integer executeUpdate(String sql) throws SQLException {
    Integer count = 0;
    Connection conn = getConnection();
    Statement statement = conn.createStatement();
    try {
      count = statement.executeUpdate(sql);
    } catch (Throwable ex) {
      //logger.error(s"$sql - ${ex.toString}")
    }

    conn.commit();
    statement.close();
    conn.close();
    return count;
  }

}
*/
