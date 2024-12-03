/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.demo;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.util.Sources;

import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Objects;
import java.util.Properties;

/**
 * @Author: Maoyf
 * @Description: 连接工具
 * @DateTime: 2023/4/19 9:08
 */
public class ConnectionUtil {

  private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

  public static Charset getDefaultCharset() {
    return DEFAULT_CHARSET;
  }

  /**
   * @Author: Maoyf
   * @Description: 关闭连接
   * @DateTime: 2023/4/18 16:42
   * @Params: [connection, statement]
   * @Return: [java.sql.Connection, java.sql.Statement]
   */
  private void close(Connection connection, Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        // ignore
      }
    }
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  /**
   * @Author: Maoyf
   * @Description: 运行SQL
   * @DateTime: 2023/4/18 17:07
   * @Params: [sql, model, fn]
   * @Return: [java.lang.String, java.lang.String, java.util.function.Consumer<java.sql.ResultSet>]
   */
  public void runSql(String sql, String model) throws SQLException {
    Connection connection = null;
    Statement statement = null;
    try {
      Properties info = new Properties();
      info.put(CalciteConnectionProperty.MODEL.name(), jsonPath(model));
      info.put(CalciteConnectionProperty.LEX.name(), "JAVA");
      info.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),"false");
      // info.put("charset", "UTF-8");
      connection = DriverManager.getConnection("jdbc:calcite:", info);
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
      statement = calciteConnection.createStatement();
//            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
//            statement = calciteConnection.createStatement();
      System.out.println("sql = " + sql + ", model = " + model);
      final ResultSet resultSet = statement.executeQuery(sql);
      output(resultSet, System.out);
    } finally {
      close(connection, statement);
    }
  }

  /**
   * @Author: Maoyf
   * @Description: 用于执行插入语句
   * @DateTime: 2023/4/20 15:35
   * @Params: [sql, model]
   * @Return: [java.lang.String, java.lang.String]
   */
  public void runSqlUpdate(String sql, String model) throws SQLException {
    Connection connection = null;
    Statement statement = null;
    try {
      Properties info = new Properties();
      info.put("model", jsonPath(model));
      info.put("lex", "JAVA");
      // info.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),"false");
      // info.put("caseSensitive",false);
      // info.put("charset", "UTF-8");
      connection = DriverManager.getConnection("jdbc:calcite:", info);
      statement = connection.createStatement();
      System.out.println("sql = " + sql + ", model = " + model);
      final int resultNum = statement.executeUpdate(sql);
      System.out.println(resultNum);
    } finally {
      close(connection, statement);
    }
  }

  /**
   * @Author: Maoyf
   * @Description: 输出流
   * @DateTime: 2023/4/19 9:31
   * @Params: [resultSet, out]
   * @Return: [java.sql.ResultSet, java.io.PrintStream]
   */
  private void output(ResultSet resultSet, PrintStream out) throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    while (resultSet.next()) {
      for (int i = 1; ; i++) {
        out.print(resultSet.getString(i));
        if (i < columnCount) {
          out.print(", ");
        } else {
          out.println();
          break;
        }
      }
    }
  }

  /**
   * @Author: Maoyf
   * @Description: get json file path
   * @DateTime: 2023/4/18 16:42
   * @Params: [model]
   * @Return: [java.lang.String]
   */
  private String jsonPath(String model) {
    return resourcePath(model + ".json");
  }

  /**
   * @Author: Maoyf
   * @Description: get absolutely path
   * @DateTime: 2023/4/18 16:42
   * @Params: [path]
   * @Return: [java.lang.String]
   */
  private String resourcePath(String path) {
    return Sources.of(Objects.requireNonNull(ConnectQueryDemo.class.getResource("/" + path))).file().getAbsolutePath();
  }
}
