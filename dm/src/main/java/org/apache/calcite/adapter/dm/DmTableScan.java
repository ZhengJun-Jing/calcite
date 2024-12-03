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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * Relational expression representing a scan of a table in a DM data source.
 */
public class DmTableScan extends TableScan implements DmRel {
  public final DmTable dmTable;

  protected DmTableScan(
      RelOptCluster cluster,
      List<RelHint> hints,
      RelOptTable table,
      DmTable dmTable,
      DmConvention dmConvention) {
    super(cluster, cluster.traitSetOf(dmConvention), hints, table);
    this.dmTable = requireNonNull(dmTable, "dmTable");
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new DmTableScan(
        getCluster(), getHints(), table, dmTable, (DmConvention) castNonNull(getConvention()));
  }

  @Override public DmImplementor.Result implement(DmImplementor implementor) {
    return implementor.result(dmTable.tableName(),
        ImmutableList.of(DmImplementor.Clause.FROM), this, null);
  }

  @Override public RelNode withHints(List<RelHint> hintList) {
    Convention convention = requireNonNull(getConvention(), "getConvention()");
    return new DmTableScan(getCluster(), hintList, getTable(), dmTable,
        (DmConvention) convention);
  }
}
