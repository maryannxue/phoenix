/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.execute.visitor;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.phoenix.compile.ListJarsQueryPlan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.TraceQueryPlan;
import org.apache.phoenix.execute.*;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.parse.JoinTableNode;

import java.sql.SQLException;
import java.util.List;

/**
 * Implementation of QueryPlanVisitor used to get the number of output rows for a QueryPlan.
 */
public class RowCountVisitor implements QueryPlanVisitor<Double> {

    // An estimate of the ratio of result data from group-by against the input data.
    private final static double GROUPING_FACTOR = 0.1;

    private final static double OUTER_JOIN_FACTOR = 1.15;
    private final static double INNER_JOIN_FACTOR = 0.85;
    private final static double SEMI_OR_ANTI_JOIN_FACTOR = 0.5;

    @Override
    public Double defaultReturn(QueryPlan plan) {
        return null;
    }

    @Override
    public Double visit(AggregatePlan plan) {
        if (plan.getGroupBy().isUngroupedAggregate()) {
            return 1.0;
        }
        if (plan.getLimit() != null) {
            return (double) plan.getLimit();
        }
        try {
            Long b = plan.getEstimatedRowsToScan();
            if (b != null) {
                double filterSelectivity = estimateFilterSelectivity(
                        stripSkipScanFilter(plan.getContext().getScan().getFilter()));
                return b.doubleValue()
                        * filterSelectivity
                        * GROUPING_FACTOR
                        * estimateFilterSelectivity(plan.getHaving());
            }
        } catch (SQLException e) {
        }

        return null;
    }

    @Override
    public Double visit(ScanPlan plan) {
        if (plan.getLimit() != null) {
            return (double) plan.getLimit();
        }
        try {
            Long b = plan.getEstimatedRowsToScan();
            if (b != null) {
                double filterSelectivity = estimateFilterSelectivity(
                        stripSkipScanFilter(plan.getContext().getScan().getFilter()));
                return b.doubleValue() * filterSelectivity;
            }
        } catch (SQLException e) {
        }

        return null;
    }

    @Override
    public Double visit(ClientAggregatePlan plan) {
        if (plan.getGroupBy().isUngroupedAggregate()) {
            return 1.0;
        }
        if (plan.getLimit() != null) {
            return (double) plan.getLimit();
        }
        Double b = plan.getDelegate().accept(this);
        if (b != null) {
            double filterSelectivity = estimateFilterSelectivity(plan.getWhere());
            return b * filterSelectivity
                    * GROUPING_FACTOR
                    * estimateFilterSelectivity(plan.getHaving());
        }

        return null;
    }

    @Override
    public Double visit(ClientScanPlan plan) {
        if (plan.getLimit() != null) {
            return (double) plan.getLimit();
        }
        Double b = plan.getDelegate().accept(this);
        if (b != null) {
            double filterSelectivity = estimateFilterSelectivity(plan.getWhere());
            return b * filterSelectivity;
        }

        return null;
    }

    @Override
    public Double visit(LiteralResultIterationPlan plan) {
        return 1.0;
    }

    @Override
    public Double visit(TupleProjectionPlan plan) {
        return plan.getDelegate().accept(this);
    }

    @Override
    public Double visit(HashJoinPlan plan) {
        Double lhsRows = plan.getDelegate().accept(this);
        if (lhsRows != null) {
            JoinTableNode.JoinType[] joinTypes = plan.getJoinInfo().getJoinTypes();
            HashJoinPlan.SubPlan[] subPlans = plan.getSubPlans();
            for (int i = 0; i < joinTypes.length; i++) {
                Double rhsRows = subPlans[i].getInnerPlan().accept(this);
                if (rhsRows == null) {
                    lhsRows = null;
                    break;
                }
                switch (joinTypes[i]) {
                    case Inner: {
                        Double rows = Math.min(lhsRows, rhsRows);
                        lhsRows = rows * INNER_JOIN_FACTOR;
                        break;
                    }
                    case Left:
                    case Right:
                    case Full: {
                        Double rows = Math.max(lhsRows, rhsRows);
                        lhsRows = rows * OUTER_JOIN_FACTOR;
                        break;
                    }
                    case Semi:
                    case Anti: {
                        lhsRows = lhsRows * SEMI_OR_ANTI_JOIN_FACTOR;
                        break;
                    }
                    default: {
                        throw new IllegalArgumentException(
                                "Unexpected join type: " + joinTypes[i]);
                    }
                }
            }
        }

        return lhsRows;
    }

    @Override
    public Double visit(SortMergeJoinPlan plan) {
        Double lhsRows = plan.getLhsPlan().accept(this);
        if (lhsRows != null) {
            Double rhsRows = plan.getRhsPlan().accept(this);
            if (rhsRows == null) {
                lhsRows = null;
            } else {
                switch (plan.getJoinType()) {
                    case Inner: {
                        Double rows = Math.min(lhsRows, rhsRows);
                        lhsRows = rows * INNER_JOIN_FACTOR;
                        break;
                    }
                    case Left:
                    case Right:
                    case Full: {
                        Double rows = Math.max(lhsRows, rhsRows);
                        lhsRows = rows * OUTER_JOIN_FACTOR;
                        break;
                    }
                    case Semi:
                    case Anti: {
                        lhsRows = lhsRows * SEMI_OR_ANTI_JOIN_FACTOR;
                        break;
                    }
                    default: {
                        throw new IllegalArgumentException(
                                "Unexpected join type: " + plan.getJoinType());
                    }
                }
            }
        }

        return lhsRows;
    }

    @Override
    public Double visit(UnionPlan plan) {
        if (plan.getLimit() != null) {
            return (double) plan.getLimit();
        }
        Double sum = 0.0;
        for (QueryPlan subPlan : plan.getSubPlans()) {
            Double b = subPlan.accept(this);
            if (b != null) {
                sum += b;
            } else {
                sum = null;
                break;
            }
        }

        return sum;
    }

    @Override
    public Double visit(UnnestArrayPlan plan) {
        return plan.getDelegate().accept(this);
    }

    @Override
    public Double visit(CorrelatePlan plan) {
        Double lhsRows = plan.getDelegate().accept(this);
        if (lhsRows != null) {
            return lhsRows * SEMI_OR_ANTI_JOIN_FACTOR;
        }

        return null;
    }

    @Override
    public Double visit(CursorFetchPlan plan) {
        return plan.getDelegate().accept(this);
    }

    @Override
    public Double visit(ListJarsQueryPlan plan) {
        return 0.0;
    }

    @Override
    public Double visit(TraceQueryPlan plan) {
        return 0.0;
    }

    private static Filter stripSkipScanFilter(Filter filter) {
        if (filter == null || filter instanceof SkipScanFilter) {
            return null;
        }
        if (filter instanceof FilterList
                && ((FilterList)filter).getOperator() == FilterList.Operator.MUST_PASS_ALL
                && ((FilterList)filter).getFilters().get(0) instanceof  SkipScanFilter) {
            List<Filter> filterList = ((FilterList)filter).getFilters();
            return new FilterList(FilterList.Operator.MUST_PASS_ALL,
                    filterList.subList(1, filterList.size()));
        } else {
            return filter;
        }
    }

    private static double estimateFilterSelectivity(Filter filter) {
        if (filter == null) {
            return 1.0;
        }
        // TODO
        return 0.5;
    }

    private static double estimateFilterSelectivity(Expression filter) {
        if (filter == null) {
            return 1.0;
        }
        // TODO
        return 0.5;
    }
}
