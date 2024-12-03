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


import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlDialect;

/**
 * Calling convention for relational operations that occur in a DM
 * database.
 *
 * <p>The convention is a slight misnomer. The operations occur in whatever
 * data-flow architecture the database uses internally. Nevertheless, the result
 * pops out in DM.
 *
 * <p>This is the only convention, thus far, that is not a singleton. Each
 * instance contains a DM schema (and therefore a data source). If Calcite is
 * working with two different databases, it would even make sense to convert
 * from "DM#A" convention to "DM#B", even though we don't do it currently.
 * (That would involve asking database B to open a database link to database
 * A.)
 *
 * <p>As a result, converter rules from and to this convention need to be
 * instantiated, at the start of planning, for each DM database in play.
 */
public class DmConvention extends Convention.Impl {
  /** Cost of a DM node versus implementing an equivalent node in a "typical"
   * calling convention. */
  public static final double COST_MULTIPLIER = 0.8d;

  public final SqlDialect dialect;
  public final Expression expression;

  public DmConvention(SqlDialect dialect, Expression expression,
                      String name) {
    super("DM." + name, DmRel.class);
    this.dialect = dialect;
    this.expression = expression;
  }

  public static DmConvention of(SqlDialect dialect, Expression expression,
                                String name) {
    return new DmConvention(dialect, expression, name);
  }

  @Override public void register(RelOptPlanner planner) {
    for (RelOptRule rule : DmRules.rules(this)) {
      planner.addRule(rule);
    }
    planner.addRule(CoreRules.FILTER_SET_OP_TRANSPOSE);
    planner.addRule(CoreRules.PROJECT_REMOVE);
  }
}
