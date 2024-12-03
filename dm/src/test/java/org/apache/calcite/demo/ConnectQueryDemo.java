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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;

import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import org.apache.calcite.sql2rel.StandardConvertletTable;

import org.apache.calcite.tools.*;

import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;


/**
 * @Author: Maoyf
 * @Description: 测试各种model链接查询功能
 * @DateTime: 2023/4/18 15:58
 */
public class ConnectQueryDemo {
//    public static void main(String[] args) throws Exception {
//        ConnectionUtil f = new ConnectionUtil();
//        f.runSql("select * from student where name like '%w%'", "redis-model");
//    }

//  @Test
//  public void testMysqlSelect() throws SQLException {
//    ConnectionUtil f = new ConnectionUtil();
//    f.runSql("select * from student where name like '%jim%'", "mysql-model");
//  }

  @Test
  public void testDmSelect() throws SQLException, ClassNotFoundException {
    ConnectionUtil f = new ConnectionUtil();
    f.runSql("select a.name,a.number,b.grade  from student a left join grade  b on a.number = b.number", "dm-model");
//    Class.forName("dm.jdbc.driver.DmDriver");
//    String url = "jdbc:dm://172.27.22.237:9006/calcite";
//    String userID = "SYSDBA";
//    String passwd = "SYSDBA";
//    Connection con = DriverManager.getConnection(url, userID, passwd);
//    PreparedStatement ps = con.prepareStatement("select name from calcite.student");
//    ResultSet rs = ps.executeQuery();
//    String name = "";
//    while (rs.next())
//    {
//      name = rs.getString("name");
//    }
//    System.out.println(name);
  }


  private static void addEnumConstant(Class<SqlDialect.DatabaseProduct> enumClass, String newConstant) throws Exception {
//    // 获取枚举常量的一个字段
//    Field valuesField = Enum.class.getDeclaredField("enumConstantDirectory");
//    valuesField.setAccessible(true);
//
//    // 获取当前枚举的常量
//    Object enumConstants = valuesField.get(enumClass);

    // 通过反射添加新常量
    Field field = enumClass.getDeclaredField("$VALUES");
    field.setAccessible(true);
    Object[] oldValues = (Object[]) field.get(null);
    Object[] newValues = new Object[oldValues.length + 1];

    // 将旧常量复制到新的数组中
    System.arraycopy(oldValues, 0, newValues, 0, oldValues.length);

    // 创建新常量并存储
    newValues[newValues.length - 1] = Enum.valueOf(enumClass, newConstant);

    // 设置新的常量数组到枚举
    field.set(null, newValues);
  }
//
  @Test
  public void testDmExtractFeilds() throws SQLException, SqlParseException, ValidationException,
      RelConversionException {
    String sql = "select a.name,a.number,b.grade  from student a left join grade  b on a.number = b.number";

    // 配置 Calcite
    Properties info = new Properties();
    info.put("model", jsonPath("dm-model"));
    info.put("lex", "JAVA");
    info.put("caseSensitive", "false");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SqlParser.Config parseConfig = SqlParser.config();;
    parseConfig = parseConfig.withCaseSensitive(false);

    FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(parseConfig)
        .defaultSchema(calciteConnection.getRootSchema().getSubSchema(calciteConnection.getSchema()))
        .build();
    Planner planner = Frameworks.getPlanner(config);

    // 解析和验证 SQL
    SqlNode sqlNode = planner.parse(sql);
    sqlNode = planner.validate(sqlNode);

    // 将 SQL 转换为 RelNode
    RelRoot relRoot = planner.rel(sqlNode);

    // 提取字段
    List<String> fields = extractFields(relRoot.project());

    // 输出字段
    fields.forEach(System.out::println);
  }

  public List<String> extractFields(RelNode relNode) {
    List<String> fieldNames = new ArrayList<>();

    // 检查节点类型
    if (relNode instanceof LogicalProject) {
      LogicalProject project = (LogicalProject) relNode;

      // 获取投影的字段名
      List<RelDataTypeField> fieldCollations = project.getRowType().getFieldList();
      for (RelDataTypeField fieldCollation : fieldCollations) {
        fieldNames.add(fieldCollation.getName());
      }

      // 递归处理子节点
      RelNode input = project.getInput();
      fieldNames.addAll(extractFields(input));

    } else {
      // 对于其他类型的节点，你可能需要根据节点类型来提取字段名
      // 或者简单地递归处理其子节点
      for (RelNode child : relNode.getInputs()) {
        fieldNames.addAll(extractFields(child));
      }
    }
    return fieldNames;
  }
//

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

//  @Test
//  public void testDmInsert() throws SQLException {
//    ConnectionUtil f = new ConnectionUtil();
//    f.runSqlUpdate("insert into student values(33,'wang wu',35)", "dm-model");
//  }
//
//  //
//  @Test
//  public void testDmDelete() throws SQLException {
//    ConnectionUtil f = new ConnectionUtil();
//    f.runSqlUpdate("delete from student where id =33", "dm-model");
//  }
//
//    @Test
//    public void testMysqlSelectCN() throws SQLException {
//        ConnectionUtil f = new ConnectionUtil();
//        f.runSql("select * from student where name like '%冯%'", "mysql-model");
//    }
//
//    @Test
//    public void testRedisSelectJson() throws SQLException {
//        ConnectionUtil f = new ConnectionUtil();
//        f.runSql("SELECT * FROM json_01 WHERE DEPTNO = 10", "redis-model");
//    }
//
//    @Test
//    public void testRedisSelectRaw() throws SQLException {
//        ConnectionUtil f = new ConnectionUtil();
//        f.runSql("SELECT * FROM raw_01", "redis-model");
//    }
//
//
//    @Test
//    public void testHiveSelect() throws SQLException {
//        ConnectionUtil f = new ConnectionUtil();
//        f.runSql("SELECT * FROM id_test", "hive-model");
//    }
//
//    // @Test
//    // void testRedisBySql() {
//    //     TABLE_MAPS.forEach((table, count) -> {
//    //         String sql = "Select count(*) as c from \"" + table + "\" where true";
//    //         sql(sql). ("C=" + count);
//    //     });
//    // }
//    //
//    // @Test
//    // void testSqlWithJoin() {
//    //     String sql = "Select a.DEPTNO, b.NAME " + "from \"csv_01\" a left join \"json_02\" b
//    " + "on a.DEPTNO=b.DEPTNO where true";
//    //     sql(sql).returnsUnordered("DEPTNO=10; NAME=\"Sales1\"");
//    // }
}
