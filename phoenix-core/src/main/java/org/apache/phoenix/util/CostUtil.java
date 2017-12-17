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
package org.apache.phoenix.util;

import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.optimize.Cost;
import org.apache.phoenix.query.QueryServices;

/**
 * Utilities for computing costs.
 *
 * Some of the methods here should eventually be replaced by a metadata framework which
 * estimates output metrics for each QueryPlan or operation, e.g. row count, byte count,
 * etc.
 */
public class CostUtil {

    // Io operations conducted in intermediate evaluations like sorting or aggregation
    // should be counted twice since they usually involve both read and write.
    private final static double IO_COST_MULTIPLIER = 2.0;

    /**
     * Estimate the cost of an aggregate.
     * @param inputBytes the number of input bytes
     * @param outputBytes the number of output bytes
     * @param groupBy the compiled GroupBy object
     * @param parallelLevel number of parallel workers or threads
     * @return the cost
     */
    public static Cost estimateAggregateCost(
            double inputBytes, double outputBytes, GroupBy groupBy, int parallelLevel) {
        double hashMapOverhead = groupBy.isOrderPreserving() ? 0 : (outputBytes * IO_COST_MULTIPLIER);
        return new Cost(0, 0, (inputBytes + hashMapOverhead) / parallelLevel);
    }

    /**
     * Estimate the cost of an order-by
     * @param inputBytes the number of input bytes
     * @param outputBytes the number of output bytes, which may be different from inputBytes
     *                    depending on whether there is a LIMIT
     * @param parallelLevel number of parallel workers or threads
     * @return the cost
     */
    public static Cost estimateOrderByCost(double inputBytes, double outputBytes, int parallelLevel) {
        return new Cost(0, 0,
                (inputBytes + outputBytes * IO_COST_MULTIPLIER) / parallelLevel);
    }

    /**
     * Estimate the parallel level of an operation
     * @param runningOnServer if the operation will be running on server side
     * @param services the QueryServices object
     * @return the parallel level
     */
    public static int estimateParallelLevel(boolean runningOnServer, QueryServices services) {
        // TODO currently return constants for simplicity, should derive from cluster config.
        return runningOnServer ? 10 : 1;
    }
}
