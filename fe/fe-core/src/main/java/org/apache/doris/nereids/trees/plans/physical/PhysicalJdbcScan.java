// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Physical jdbc scan for external catalog.
 */
public class PhysicalJdbcScan extends PhysicalCatalogRelation {

    /**
     * Constructor for PhysicalJdbcScan.
     */
    public PhysicalJdbcScan(RelationId id, TableIf table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties) {
        this(id, table, qualifier, groupExpression, logicalProperties,
                null, null, ImmutableList.of());
    }

    /**
     * Constructor for PhysicalJdbcScan.
     */
    public PhysicalJdbcScan(RelationId id, TableIf table, List<String> qualifier,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties, Statistics statistics,
            Collection<Slot> operativeSlots) {
        super(id, PlanType.PHYSICAL_JDBC_SCAN, table, qualifier, groupExpression,
                logicalProperties, physicalProperties, statistics, operativeSlots);
    }

    @Override
    public String toString() {
        String rfV2 = "";
        if (!runtimeFiltersV2.isEmpty()) {
            rfV2 = runtimeFiltersV2.toString();
        }
        return Utils.toSqlString("PhysicalJdbcScan",
            "qualified", Utils.qualifiedName(qualifier, table.getName()),
            "output", getOutput(),
            "RFV2", rfV2,
            "stats", statistics
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalJdbcScan(this, context);
    }

    @Override
    public PhysicalJdbcScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalJdbcScan(relationId, table, qualifier, groupExpression, getLogicalProperties());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalJdbcScan(relationId, table, qualifier, groupExpression, logicalProperties.get());
    }

    @Override
    public PhysicalJdbcScan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
                                                           Statistics statistics) {
        return new PhysicalJdbcScan(relationId, table, qualifier, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, operativeSlots);
    }
}
