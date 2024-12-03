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

import org.apache.calcite.adapter.dm.DmConvention;
import org.apache.calcite.adapter.dm.DmImplementor;
import org.apache.calcite.adapter.dm.DmToEnumerableConverterRule;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

/**
 * Rules and relational operators for
 * {@link DmConvention}
 * calling convention.
 */
public class DmRules {
  private DmRules() {
  }

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  static final RelFactories.ProjectFactory PROJECT_FACTORY =
      (input, hints, projects, fieldNames, variablesSet) -> {
        checkArgument(variablesSet.isEmpty(),
            "DmProject does not allow variables");
        final RelOptCluster cluster = input.getCluster();
        final RelDataType rowType =
            RexUtil.createStructType(cluster.getTypeFactory(), projects,
                fieldNames, SqlValidatorUtil.F_SUGGESTER);
        return new DmProject(cluster, input.getTraitSet(), input, projects,
            rowType);
      };

  static final RelFactories.FilterFactory FILTER_FACTORY =
      (input, condition, variablesSet) -> {
        checkArgument(variablesSet.isEmpty(),
            "DmFilter does not allow variables");
        return new DmFilter(input.getCluster(),
            input.getTraitSet(), input, condition);
      };

  static final RelFactories.JoinFactory JOIN_FACTORY =
      (left, right, hints, condition, variablesSet, joinType, semiJoinDone) -> {
        final RelOptCluster cluster = left.getCluster();
        final RelTraitSet traitSet =
            cluster.traitSetOf(
                requireNonNull(left.getConvention(), "left.getConvention()"));
        try {
          return new DmJoin(cluster, traitSet, left, right, condition,
              variablesSet, joinType);
        } catch (InvalidRelException e) {
          throw new AssertionError(e);
        }
      };

  static final RelFactories.CorrelateFactory CORRELATE_FACTORY =
      (left, right, hints, correlationId, requiredColumns, joinType) -> {
        throw new UnsupportedOperationException("DmCorrelate");
      };

  public static final RelFactories.SortFactory SORT_FACTORY =
      (input, collation, offset, fetch) -> {
        throw new UnsupportedOperationException("DmSort");
      };

  public static final RelFactories.ExchangeFactory EXCHANGE_FACTORY =
      (input, distribution) -> {
        throw new UnsupportedOperationException("DmExchange");
      };

  public static final RelFactories.SortExchangeFactory SORT_EXCHANGE_FACTORY =
      (input, distribution, collation) -> {
        throw new UnsupportedOperationException("DmSortExchange");
      };

  public static final RelFactories.AggregateFactory AGGREGATE_FACTORY =
      (input, hints, groupSet, groupSets, aggCalls) -> {
        final RelOptCluster cluster = input.getCluster();
        final RelTraitSet traitSet =
            cluster.traitSetOf(
                requireNonNull(input.getConvention(), "input.getConvention()"));
        try {
          return new DmAggregate(cluster, traitSet, input, groupSet,
              groupSets, aggCalls);
        } catch (InvalidRelException e) {
          throw new AssertionError(e);
        }
      };

  public static final RelFactories.MatchFactory MATCH_FACTORY =
      (input, pattern, rowType, strictStart, strictEnd, patternDefinitions,
          measures, after, subsets, allRows, partitionKeys, orderKeys,
          interval) -> {
        throw new UnsupportedOperationException("DmMatch");
      };

  public static final RelFactories.SetOpFactory SET_OP_FACTORY =
      (kind, inputs, all) -> {
        RelNode input = inputs.get(0);
        RelOptCluster cluster = input.getCluster();
        final RelTraitSet traitSet =
            cluster.traitSetOf(
                requireNonNull(input.getConvention(), "input.getConvention()"));
        switch (kind) {
        case UNION:
          return new DmUnion(cluster, traitSet, inputs, all);
        case INTERSECT:
          return new DmIntersect(cluster, traitSet, inputs, all);
        case EXCEPT:
          return new DmMinus(cluster, traitSet, inputs, all);
        default:
          throw new AssertionError("unknown: " + kind);
        }
      };

  public static final RelFactories.ValuesFactory VALUES_FACTORY =
      (cluster, rowType, tuples) -> {
        throw new UnsupportedOperationException();
      };

  public static final RelFactories.TableScanFactory TABLE_SCAN_FACTORY =
      (toRelContext, table) -> {
        throw new UnsupportedOperationException();
      };

  public static final RelFactories.SnapshotFactory SNAPSHOT_FACTORY =
      (input, period) -> {
        throw new UnsupportedOperationException();
      };

  /** A {@link RelBuilderFactory} that creates a {@link RelBuilder} that will
   * create DM relational expressions for everything. */
  public static final RelBuilderFactory DM_BUILDER =
      RelBuilder.proto(
          Contexts.of(PROJECT_FACTORY,
              FILTER_FACTORY,
              JOIN_FACTORY,
              SORT_FACTORY,
              EXCHANGE_FACTORY,
              SORT_EXCHANGE_FACTORY,
              AGGREGATE_FACTORY,
              MATCH_FACTORY,
              SET_OP_FACTORY,
              VALUES_FACTORY,
              TABLE_SCAN_FACTORY,
              SNAPSHOT_FACTORY));

  /** Creates a list of rules with the given DM convention instance. */
  public static List<RelOptRule> rules(DmConvention out) {
    final ImmutableList.Builder<RelOptRule> b = ImmutableList.builder();
    foreachRule(out, b::add);
    return b.build();
  }

  /** Creates a list of rules with the given DM convention instance
   * and builder factory. */
  public static List<RelOptRule> rules(DmConvention out,
      RelBuilderFactory relBuilderFactory) {
    final ImmutableList.Builder<RelOptRule> b = ImmutableList.builder();
    foreachRule(out, r ->
        b.add(r.config.withRelBuilderFactory(relBuilderFactory).toRule()));
    return b.build();
  }

  private static void foreachRule(DmConvention out,
      Consumer<RelRule<?>> consumer) {
    consumer.accept(DmToEnumerableConverterRule.create(out));
    consumer.accept(DmJoinRule.create(out));
    consumer.accept(DmProjectRule.create(out));
    consumer.accept(DmFilterRule.create(out));
    consumer.accept(DmAggregateRule.create(out));
    consumer.accept(DmSortRule.create(out));
    consumer.accept(DmUnionRule.create(out));
    consumer.accept(DmIntersectRule.create(out));
    consumer.accept(DmMinusRule.create(out));
    consumer.accept(DmTableModificationRule.create(out));
    consumer.accept(DmValuesRule.create(out));
  }

  /** Abstract base class for rule that converts to DM. */
  abstract static class DmConverterRule extends ConverterRule {
    protected DmConverterRule(Config config) {
      super(config);
    }
  }

  /** Rule that converts a join to DM. */
  public static class DmJoinRule extends DmConverterRule {
    /** Creates a DmJoinRule. */
    public static DmJoinRule create(DmConvention out) {
      return Config.INSTANCE
          .withConversion(Join.class, Convention.NONE, out, "DmJoinRule")
          .withRuleFactory(DmJoinRule::new)
          .toRule(DmJoinRule.class);
    }

    /** Called from the Config. */
    protected DmJoinRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final Join join = (Join) rel;
      switch (join.getJoinType()) {
      case SEMI:
      case ANTI:
        // It's not possible to convert semi-joins or anti-joins. They have fewer columns
        // than regular joins.
        return null;
      default:
        return convert(join, true);
      }
    }

    /**
     * Converts a {@code Join} into a {@code DmJoin}.
     *
     * @param join Join operator to convert
     * @param convertInputTraits Whether to convert input to {@code join}'s
     *                            DM convention
     * @return A new DmJoin
     */
    public @Nullable RelNode convert(Join join, boolean convertInputTraits) {
      final List<RelNode> newInputs = new ArrayList<>();
      for (RelNode input : join.getInputs()) {
        if (convertInputTraits && input.getConvention() != getOutTrait()) {
          input =
              convert(input,
                  input.getTraitSet().replace(out));
        }
        newInputs.add(input);
      }
      if (convertInputTraits && !canJoinOnCondition(join.getCondition())) {
        return null;
      }
      try {
        return new DmJoin(
            join.getCluster(),
            join.getTraitSet().replace(out),
            newInputs.get(0),
            newInputs.get(1),
            join.getCondition(),
            join.getVariablesSet(),
            join.getJoinType());
      } catch (InvalidRelException e) {
        LOGGER.debug(e.toString());
        return null;
      }
    }

    /**
     * Returns whether a condition is supported by {@link DmJoin}.
     *
     * <p>Corresponds to the capabilities of
     * {@link SqlImplementor#convertConditionToSqlNode}.
     *
     * @param node Condition
     * @return Whether condition is supported
     */
    private static boolean canJoinOnCondition(RexNode node) {
      final List<RexNode> operands;
      switch (node.getKind()) {
      case DYNAMIC_PARAM:
      case INPUT_REF:
      case LITERAL:
        // literal on a join condition would be TRUE or FALSE
        return true;
      case AND:
      case OR:
      case IS_NULL:
      case IS_NOT_NULL:
      case IS_TRUE:
      case IS_NOT_TRUE:
      case IS_FALSE:
      case IS_NOT_FALSE:
      case EQUALS:
      case NOT_EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case IS_NOT_DISTINCT_FROM:
      case CAST:
        operands = ((RexCall) node).getOperands();
        for (RexNode operand : operands) {
          if (!canJoinOnCondition(operand)) {
            return false;
          }
        }
        return true;
      default:
        return false;
      }
    }

    @Override public boolean matches(RelOptRuleCall call) {
      Join join = call.rel(0);
      JoinRelType joinType = join.getJoinType();
      return ((DmConvention) getOutConvention()).dialect.supportsJoinType(joinType);
    }
  }

  /** Join operator implemented in DM convention. */
  public static class DmJoin extends Join implements DmRel {
    /** Creates a DmJoin. */
    public DmJoin(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode left, RelNode right, RexNode condition,
        Set<CorrelationId> variablesSet, JoinRelType joinType)
        throws InvalidRelException {
      super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
    }

    @Deprecated // to be removed before 2.0
    protected DmJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        Set<String> variablesStopped)
        throws InvalidRelException {
      this(cluster, traitSet, left, right, condition,
          CorrelationId.setOf(variablesStopped), joinType);
    }

    @Override public DmJoin copy(RelTraitSet traitSet, RexNode condition,
        RelNode left, RelNode right, JoinRelType joinType,
        boolean semiJoinDone) {
      try {
        return new DmJoin(getCluster(), traitSet, left, right,
            condition, variablesSet, joinType);
      } catch (InvalidRelException e) {
        // Semantic error not possible. Must be a bug. Convert to
        // internal error.
        throw new AssertionError(e);
      }
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      // We always "build" the
      double rowCount = mq.getRowCount(this);

      return planner.getCostFactory().makeCost(rowCount, 0, 0);
    }

    @Override public double estimateRowCount(RelMetadataQuery mq) {
      final double leftRowCount = mq.getRowCount(left);
      final double rightRowCount = mq.getRowCount(right);
      return Math.max(leftRowCount, rightRowCount);
    }

    @Override public DmImplementor.Result implement(DmImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /** Calc operator implemented in DM convention.
   *
   * @see org.apache.calcite.rel.core.Calc
   * */
  @Deprecated // to be removed before 2.0
  public static class DmCalc extends SingleRel implements DmRel {
    private final RexProgram program;

    public DmCalc(RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RexProgram program) {
      super(cluster, traitSet, input);
      assert getConvention() instanceof DmConvention;
      this.program = program;
      this.rowType = program.getOutputRowType();
    }

    @Deprecated // to be removed before 2.0
    public DmCalc(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
        RexProgram program, int flags) {
      this(cluster, traitSet, input, program);
      Util.discard(flags);
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
      return program.explainCalc(super.explainTerms(pw));
    }

    @Override public double estimateRowCount(RelMetadataQuery mq) {
      return RelMdUtil.estimateFilteredRows(getInput(), program, mq);
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      double dRows = mq.getRowCount(this);
      double dCpu = mq.getRowCount(getInput())
          * program.getExprCount();
      double dIo = 0;
      return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new DmCalc(getCluster(), traitSet, sole(inputs), program);
    }

    @Override public DmImplementor.Result implement(DmImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link Project} to
   * an {@link DmRules.DmProject}.
   */
  public static class DmProjectRule extends DmConverterRule {
    /** Creates a DmProjectRule. */
    public static DmProjectRule create(DmConvention out) {
      return Config.INSTANCE
          .withConversion(Project.class, project ->
                  (out.dialect.supportsWindowFunctions()
                      || !project.containsOver())
                      && !userDefinedFunctionInProject(project),
              Convention.NONE, out, "DmProjectRule")
          .withRuleFactory(DmProjectRule::new)
          .toRule(DmProjectRule.class);
    }

    /** Called from the Config. */
    protected DmProjectRule(Config config) {
      super(config);
    }

    private static boolean userDefinedFunctionInProject(Project project) {
      CheckingUserDefinedFunctionVisitor visitor = new CheckingUserDefinedFunctionVisitor();
      for (RexNode node : project.getProjects()) {
        node.accept(visitor);
        if (visitor.containsUserDefinedFunction()) {
          return true;
        }
      }
      return false;
    }

    @Override public boolean matches(RelOptRuleCall call) {
      Project project = call.rel(0);
      return project.getVariablesSet().isEmpty();
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final Project project = (Project) rel;

      return new DmProject(
          rel.getCluster(),
          rel.getTraitSet().replace(out),
          convert(
              project.getInput(),
              project.getInput().getTraitSet().replace(out)),
          project.getProjects(),
          project.getRowType());
    }
  }

  /** Implementation of {@link Project} in
   * {@link DmConvention dm calling convention}. */
  public static class DmProject
      extends Project
      implements DmRel {
    public DmProject(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<? extends RexNode> projects,
        RelDataType rowType) {
      super(cluster, traitSet, ImmutableList.of(), input, projects, rowType, ImmutableSet.of());
      assert getConvention() instanceof DmConvention;
    }

    @Deprecated // to be removed before 2.0
    public DmProject(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode input, List<RexNode> projects, RelDataType rowType, int flags) {
      this(cluster, traitSet, input, projects, rowType);
      Util.discard(flags);
    }

    @Override public DmProject copy(RelTraitSet traitSet, RelNode input,
        List<RexNode> projects, RelDataType rowType) {
      return new DmProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      RelOptCost cost = super.computeSelfCost(planner, mq);
      if (cost == null) {
        return null;
      }
      return cost.multiplyBy(DmConvention.COST_MULTIPLIER);
    }

    @Override public DmImplementor.Result implement(DmImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link Filter} to
   * an {@link DmRules.DmFilter}.
   */
  public static class DmFilterRule extends DmConverterRule {
    /** Creates a DmFilterRule. */
    public static DmFilterRule create(DmConvention out) {
      return Config.INSTANCE
          .withConversion(Filter.class, r -> !userDefinedFunctionInFilter(r),
              Convention.NONE, out, "DmFilterRule")
          .withRuleFactory(DmFilterRule::new)
          .toRule(DmFilterRule.class);
    }

    /** Called from the Config. */
    protected DmFilterRule(Config config) {
      super(config);
    }

    private static boolean userDefinedFunctionInFilter(Filter filter) {
      CheckingUserDefinedFunctionVisitor visitor = new CheckingUserDefinedFunctionVisitor();
      filter.getCondition().accept(visitor);
      return visitor.containsUserDefinedFunction();
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final Filter filter = (Filter) rel;

      return new DmFilter(
          rel.getCluster(),
          rel.getTraitSet().replace(out),
          convert(filter.getInput(),
              filter.getInput().getTraitSet().replace(out)),
          filter.getCondition());
    }
  }

  /** Implementation of {@link Filter} in
   * {@link DmConvention dm calling convention}. */
  public static class DmFilter extends Filter implements DmRel {
    public DmFilter(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RexNode condition) {
      super(cluster, traitSet, input, condition);
      assert getConvention() instanceof DmConvention;
    }

    @Override public DmFilter copy(RelTraitSet traitSet, RelNode input,
        RexNode condition) {
      return new DmFilter(getCluster(), traitSet, input, condition);
    }

    @Override public DmImplementor.Result implement(DmImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link Aggregate}
   * to a {@link DmRules.DmAggregate}.
   */
  public static class DmAggregateRule extends DmConverterRule {
    /** Creates a DmAggregateRule. */
    public static DmAggregateRule create(DmConvention out) {
      return Config.INSTANCE
          .withConversion(Aggregate.class, Convention.NONE, out,
              "DmAggregateRule")
          .withRuleFactory(DmAggregateRule::new)
          .toRule(DmAggregateRule.class);
    }

    /** Called from the Config. */
    protected DmAggregateRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final Aggregate agg = (Aggregate) rel;
      if (agg.getGroupSets().size() != 1) {
        // GROUPING SETS not supported; see
        // [CALCITE-734] Push GROUPING SETS to underlying SQL via DM adapter
        return null;
      }
      final RelTraitSet traitSet =
          agg.getTraitSet().replace(out);
      try {
        return new DmAggregate(rel.getCluster(), traitSet,
            convert(agg.getInput(), out), agg.getGroupSet(),
            agg.getGroupSets(), agg.getAggCallList());
      } catch (InvalidRelException e) {
        LOGGER.debug(e.toString());
        return null;
      }
    }
  }

  /** Returns whether this DM data source can implement a given aggregate
   * function. */
  private static boolean canImplement(AggregateCall aggregateCall,
      SqlDialect sqlDialect) {
    return sqlDialect.supportsAggregateFunction(
        aggregateCall.getAggregation().getKind())
        && aggregateCall.distinctKeys == null;
  }

  /** Aggregate operator implemented in DM convention. */
  public static class DmAggregate extends Aggregate implements DmRel {
    public DmAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        @Nullable List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls)
        throws InvalidRelException {
      super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
      assert getConvention() instanceof DmConvention;
      assert this.groupSets.size() == 1 : "Grouping sets not supported";
      final SqlDialect dialect = ((DmConvention) getConvention()).dialect;
      for (AggregateCall aggCall : aggCalls) {
        if (!canImplement(aggCall, dialect)) {
          throw new InvalidRelException("cannot implement aggregate function "
              + aggCall);
        }
        if (aggCall.hasFilter() && !dialect.supportsAggregateFunctionFilter()) {
          throw new InvalidRelException("dialect does not support aggregate "
              + "functions FILTER clauses");
        }
      }
    }

    @Deprecated // to be removed before 2.0
    public DmAggregate(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode input, boolean indicator, ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls)
        throws InvalidRelException {
      this(cluster, traitSet, input, groupSet, groupSets, aggCalls);
      checkIndicator(indicator);
    }

    @Override public DmAggregate copy(RelTraitSet traitSet, RelNode input,
        ImmutableBitSet groupSet,
        @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
      try {
        return new DmAggregate(getCluster(), traitSet, input,
            groupSet, groupSets, aggCalls);
      } catch (InvalidRelException e) {
        // Semantic error not possible. Must be a bug. Convert to
        // internal error.
        throw new AssertionError(e);
      }
    }

    @Override public DmImplementor.Result implement(DmImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link Sort} to an
   * {@link DmRules.DmSort}.
   */
  public static class DmSortRule extends DmConverterRule {
    /** Creates a DmSortRule. */
    public static DmSortRule create(DmConvention out) {
      return Config.INSTANCE
          .withConversion(Sort.class, Convention.NONE, out, "DmSortRule")
          .withRuleFactory(DmSortRule::new)
          .toRule(DmSortRule.class);
    }

    /** Called from the Config. */
    protected DmSortRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      return convert((Sort) rel, true);
    }

    /**
     * Converts a {@code Sort} into a {@code DmSort}.
     *
     * @param sort Sort operator to convert
     * @param convertInputTraits Whether to convert input to {@code sort}'s
     *                            DM convention
     * @return A new DmSort
     */
    public RelNode convert(Sort sort, boolean convertInputTraits) {
      final RelTraitSet traitSet = sort.getTraitSet().replace(out);

      final RelNode input;
      if (convertInputTraits) {
        final RelTraitSet inputTraitSet = sort.getInput().getTraitSet().replace(out);
        input = convert(sort.getInput(), inputTraitSet);
      } else {
        input = sort.getInput();
      }

      return new DmSort(sort.getCluster(), traitSet,
          input, sort.getCollation(), sort.offset, sort.fetch);
    }
  }

  /** Sort operator implemented in DM convention. */
  public static class DmSort
      extends Sort
      implements DmRel {
    public DmSort(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RelCollation collation,
        @Nullable RexNode offset,
        @Nullable RexNode fetch) {
      super(cluster, traitSet, input, collation, offset, fetch);
      assert getConvention() instanceof DmConvention;
      assert getConvention() == input.getConvention();
    }

    @Override public DmSort copy(RelTraitSet traitSet, RelNode newInput,
        RelCollation newCollation, @Nullable RexNode offset, @Nullable RexNode fetch) {
      return new DmSort(getCluster(), traitSet, newInput, newCollation,
          offset, fetch);
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      RelOptCost cost = super.computeSelfCost(planner, mq);
      if (cost == null) {
        return null;
      }
      return cost.multiplyBy(0.9);
    }

    @Override public DmImplementor.Result implement(DmImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert an {@link Union} to a
   * {@link DmRules.DmUnion}.
   */
  public static class DmUnionRule extends DmConverterRule {
    /** Creates a DmUnionRule. */
    public static DmUnionRule create(DmConvention out) {
      return Config.INSTANCE
          .withConversion(Union.class, Convention.NONE, out, "DmUnionRule")
          .withRuleFactory(DmUnionRule::new)
          .toRule(DmUnionRule.class);
    }

    /** Called from the Config. */
    protected DmUnionRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final Union union = (Union) rel;
      final RelTraitSet traitSet =
          union.getTraitSet().replace(out);
      return new DmUnion(rel.getCluster(), traitSet,
          convertList(union.getInputs(), out), union.all);
    }
  }

  /** Union operator implemented in DM convention. */
  public static class DmUnion extends Union implements DmRel {
    public DmUnion(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
    }

    @Override public DmUnion copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new DmUnion(getCluster(), traitSet, inputs, all);
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      RelOptCost cost = super.computeSelfCost(planner, mq);
      if (cost == null) {
        return null;
      }
      return cost.multiplyBy(DmConvention.COST_MULTIPLIER);
    }

    @Override public DmImplementor.Result implement(DmImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link Intersect}
   * to a {@link DmRules.DmIntersect}.
   */
  public static class DmIntersectRule extends DmConverterRule {
    /** Creates a DmIntersectRule. */
    public static DmIntersectRule create(DmConvention out) {
      return Config.INSTANCE
          .withConversion(Intersect.class, Convention.NONE, out,
              "DmIntersectRule")
          .withRuleFactory(DmIntersectRule::new)
          .toRule(DmIntersectRule.class);
    }

    /** Called from the Config. */
    protected DmIntersectRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final Intersect intersect = (Intersect) rel;
      if (intersect.all) {
        return null; // INTERSECT ALL not implemented
      }
      final RelTraitSet traitSet =
          intersect.getTraitSet().replace(out);
      return new DmIntersect(rel.getCluster(), traitSet,
          convertList(intersect.getInputs(), out), false);
    }
  }

  /** Intersect operator implemented in DM convention. */
  public static class DmIntersect
      extends Intersect
      implements DmRel {
    public DmIntersect(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
    }

    @Override public DmIntersect copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new DmIntersect(getCluster(), traitSet, inputs, all);
    }

    @Override public DmImplementor.Result implement(DmImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /**
   * Rule to convert a {@link Minus} to a
   * {@link DmRules.DmMinus}.
   */
  public static class DmMinusRule extends DmConverterRule {
    /** Creates a DmMinusRule. */
    public static DmMinusRule create(DmConvention out) {
      return Config.INSTANCE
          .withConversion(Minus.class, Convention.NONE, out, "DmMinusRule")
          .withRuleFactory(DmMinusRule::new)
          .toRule(DmMinusRule.class);
    }

    /** Called from the Config. */
    protected DmMinusRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final Minus minus = (Minus) rel;
      if (minus.all) {
        return null; // EXCEPT ALL not implemented
      }
      final RelTraitSet traitSet =
          rel.getTraitSet().replace(out);
      return new DmMinus(rel.getCluster(), traitSet,
          convertList(minus.getInputs(), out), false);
    }
  }

  /** Minus operator implemented in DM convention. */
  public static class DmMinus extends Minus implements DmRel {
    public DmMinus(RelOptCluster cluster, RelTraitSet traitSet,
        List<RelNode> inputs, boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
    }

    @Override public DmMinus copy(RelTraitSet traitSet, List<RelNode> inputs,
        boolean all) {
      return new DmMinus(getCluster(), traitSet, inputs, all);
    }

    @Override public DmImplementor.Result implement(DmImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /** Rule that converts a table-modification to DM. */
  public static class DmTableModificationRule extends DmConverterRule {
    /** Creates a DmToEnumerableConverterRule. */
    public static DmTableModificationRule create(DmConvention out) {
      return Config.INSTANCE
          .withConversion(TableModify.class, Convention.NONE, out,
              "DmTableModificationRule")
          .withRuleFactory(DmTableModificationRule::new)
          .toRule(DmTableModificationRule.class);
    }

    /** Called from the Config. */
    protected DmTableModificationRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final TableModify modify =
          (TableModify) rel;
      final ModifiableTable modifiableTable =
          modify.getTable().unwrap(ModifiableTable.class);
      if (modifiableTable == null) {
        return null;
      }
      final RelTraitSet traitSet =
          modify.getTraitSet().replace(out);
      return new DmTableModify(
          modify.getCluster(), traitSet,
          modify.getTable(),
          modify.getCatalogReader(),
          convert(modify.getInput(), traitSet),
          modify.getOperation(),
          modify.getUpdateColumnList(),
          modify.getSourceExpressionList(),
          modify.isFlattened());
    }
  }

  /** Table-modification operator implemented in DM convention. */
  public static class DmTableModify extends TableModify implements DmRel {
    public DmTableModify(RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        Prepare.CatalogReader catalogReader,
        RelNode input,
        Operation operation,
        @Nullable List<String> updateColumnList,
        @Nullable List<RexNode> sourceExpressionList,
        boolean flattened) {
      super(cluster, traitSet, table, catalogReader, input, operation,
          updateColumnList, sourceExpressionList, flattened);
      assert input.getConvention() instanceof DmConvention;
      assert getConvention() instanceof DmConvention;
      final ModifiableTable modifiableTable =
          table.unwrap(ModifiableTable.class);
      if (modifiableTable == null) {
        throw new AssertionError(); // TODO: user error in validator
      }
      Expression expression = table.getExpression(Queryable.class);
      if (expression == null) {
        throw new AssertionError(); // TODO: user error in validator
      }
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      RelOptCost cost = super.computeSelfCost(planner, mq);
      if (cost == null) {
        return null;
      }
      return cost.multiplyBy(.1);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new DmTableModify(
          getCluster(), traitSet, getTable(), getCatalogReader(),
          sole(inputs), getOperation(), getUpdateColumnList(),
          getSourceExpressionList(), isFlattened());
    }

    @Override public DmImplementor.Result implement(DmImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /** Rule that converts a values operator to DM. */
  public static class DmValuesRule extends DmConverterRule {
    /** Creates a DmValuesRule. */
    public static DmValuesRule create(DmConvention out) {
      return Config.INSTANCE
          .withConversion(Values.class, Convention.NONE, out, "DmValuesRule")
          .withRuleFactory(DmValuesRule::new)
          .toRule(DmValuesRule.class);
    }

    /** Called from the Config. */
    protected DmValuesRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      Values values = (Values) rel;
      return new DmValues(values.getCluster(), values.getRowType(),
          values.getTuples(), values.getTraitSet().replace(out));
    }
  }

  /** Values operator implemented in DM convention. */
  public static class DmValues extends Values implements DmRel {
    DmValues(RelOptCluster cluster, RelDataType rowType,
        ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traitSet) {
      super(cluster, rowType, tuples, traitSet);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert inputs.isEmpty();
      return new DmValues(getCluster(), getRowType(), tuples, traitSet);
    }

    @Override public DmImplementor.Result implement(DmImplementor implementor) {
      return implementor.implement(this);
    }
  }

  /** Visitor that checks whether part of a projection is a user-defined
   * function (UDF). */
  private static class CheckingUserDefinedFunctionVisitor
      extends RexVisitorImpl<Void> {

    private boolean containsUsedDefinedFunction = false;

    CheckingUserDefinedFunctionVisitor() {
      super(true);
    }

    public boolean containsUserDefinedFunction() {
      return containsUsedDefinedFunction;
    }

    @Override public Void visitCall(RexCall call) {
      SqlOperator operator = call.getOperator();
      if (operator instanceof SqlFunction
          && ((SqlFunction) operator).getFunctionType().isUserDefined()) {
        containsUsedDefinedFunction |= true;
      }
      return super.visitCall(call);
    }

  }

}
