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
package org.apache.calcite.adapter.dm;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.LibraryOperator;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.commons.dbcp2.BasicDataSource;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.sql.DataSource;
import java.util.List;



/**
 * A <code>SqlDialect</code> implementation for the Dm database.
 */
public class DmSqlDialect extends SqlDialect {
  /**
   * DmDB type system.
   */
  private static final RelDataTypeSystem Dm_TYPE_SYSTEM =
          new RelDataTypeSystemImpl() {
            @Override
            public int getMaxPrecision(SqlTypeName typeName) {
              switch (typeName) {
                case VARCHAR:
                  // Maximum size of 4000 bytes for varchar2.
                  return 4000;
                default:
                  return super.getMaxPrecision(typeName);
              }
            }
          };

//  public static final Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
//          .withDatabaseProduct(DatabaseProduct.ORACLE)
//          .withIdentifierQuoteString("\"")
//          .withDataTypeSystem(Dm_TYPE_SYSTEM);
//
//  public static final SqlDialect DEFAULT = new DmSqlDialect(DEFAULT_CONTEXT);

  private final int majorVersion;

  /**
   * Creates an DmSqlDialect.
   */
  public DmSqlDialect(Context context) {
    super(context);
    this.majorVersion = context.databaseMajorVersion();
  }

  @Override
  public boolean supportsApproxCountDistinct() {
    return true;
  }

  @Override
  public boolean supportsCharSet() {
    return false;
  }

  @Override
  public boolean supportBooleanCaseWhen() {
    return majorVersion >= 23;
  }

  @Override
  public boolean supportsDataType(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return false;
      default:
        return super.supportsDataType(type);
    }
  }

  @Override
  public @Nullable SqlNode getCastSpec(RelDataType type) {
    String castSpec;
    switch (type.getSqlTypeName()) {
      case SMALLINT:
        castSpec = "NUMBER(5)";
        break;
      case INTEGER:
        castSpec = "NUMBER(10)";
        break;
      case BIGINT:
        castSpec = "NUMBER(19)";
        break;
      case DOUBLE:
        castSpec = "DOUBLE PRECISION";
        break;
      default:
        return super.getCastSpec(type);
    }

    return new SqlDataTypeSpec(
            new SqlAlienSystemTypeNameSpec(castSpec, type.getSqlTypeName(), SqlParserPos.ZERO),
            SqlParserPos.ZERO);
  }

  @Override
  protected boolean allowsAs() {
    return false;
  }

  @Override
  public boolean supportsAliasedValues() {
    return false;
  }

  @Override
  public void unparseBoolLiteral(SqlWriter writer,
                                 SqlLiteral literal, int leftPrec, int rightPrec) {
    Boolean value = (Boolean) literal.getValue();
    if (value == null || majorVersion >= 23) {
      super.unparseBoolLiteral(writer, literal, leftPrec, rightPrec);
      return;
    }
    // low version Dm not support bool literal
    final SqlWriter.Frame frame = writer.startList("(", ")");
    writer.literal("1");
    writer.sep(SqlStdOperatorTable.EQUALS.getName());
    writer.literal(value ? "1" : "0");
    writer.endList(frame);
  }

  @Override
  public void unparseDateTimeLiteral(SqlWriter writer,
                                     SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
    if (literal instanceof SqlTimestampLiteral) {
      writer.literal("TO_TIMESTAMP('"
              + literal.toFormattedString() + "', 'YYYY-MM-DD HH24:MI:SS.FF')");
    } else if (literal instanceof SqlDateLiteral) {
      writer.literal("TO_DATE('"
              + literal.toFormattedString() + "', 'YYYY-MM-DD')");
    } else if (literal instanceof SqlTimeLiteral) {
      writer.literal("TO_TIME('"
              + literal.toFormattedString() + "', 'HH24:MI:SS.FF')");
    } else {
      super.unparseDateTimeLiteral(writer, literal, leftPrec, rightPrec);
    }
  }

  @Override
  public List<String> getSingleRowTableName() {
    return ImmutableList.of("DUAL");
  }

  @Override
  public void unparseCall(SqlWriter writer, SqlCall call,
                          int leftPrec, int rightPrec) {
    if (call.getOperator() == SqlStdOperatorTable.SUBSTRING) {
//      SqlUtil.unparseFunctionSyntax(SqlLibraryOperators.SUBSTR_ORACLE, writer,
//              call, false);
    } else {
      switch (call.getKind()) {
        case POSITION:
          final SqlWriter.Frame frame = writer.startFunCall("INSTR");
          writer.sep(",");
          call.operand(1).unparse(writer, leftPrec, rightPrec);
          writer.sep(",");
          call.operand(0).unparse(writer, leftPrec, rightPrec);
          if (3 == call.operandCount()) {
            writer.sep(",");
            call.operand(2).unparse(writer, leftPrec, rightPrec);
          }
          if (4 == call.operandCount()) {
            writer.sep(",");
            call.operand(2).unparse(writer, leftPrec, rightPrec);
            writer.sep(",");
            call.operand(3).unparse(writer, leftPrec, rightPrec);
          }
          writer.endFunCall(frame);
          break;
        case FLOOR:
          if (call.operandCount() != 2) {
            super.unparseCall(writer, call, leftPrec, rightPrec);
            return;
          }

          final SqlLiteral timeUnitNode = call.operand(1);
          final TimeUnitRange timeUnit = timeUnitNode.getValueAs(TimeUnitRange.class);

          SqlCall call2 =
                  SqlFloorFunction.replaceTimeUnitOperand(call, timeUnit.name(),
                          timeUnitNode.getParserPosition());
          SqlFloorFunction.unparseDatetimeFunction(writer, call2, "TRUNC", true);
          break;

        default:
          super.unparseCall(writer, call, leftPrec, rightPrec);
      }
    }
  }

  @Override
  public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
                                 @Nullable SqlNode fetch) {
    // majorVersion in SqlDialect.EMPTY_CONTEXT is -1 by default
    if (this.majorVersion != -1 && this.majorVersion < 12) {
      throw new RuntimeException("Lower Dm version(<12) doesn't support offset/fetch syntax!");
    }
    super.unparseOffsetFetch(writer, offset, fetch);
  }

}
