package org.apache.iotdb.infludb.qp.logical.crud;



/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.iotdb.infludb.qp.constant.SQLConstant;
import org.apache.iotdb.infludb.query.expression.Expression;
import org.apache.iotdb.infludb.query.expression.ResultColumn;
import org.apache.iotdb.infludb.query.expression.unary.FunctionExpression;
import org.apache.iotdb.infludb.query.expression.unary.NodeExpression;

import java.util.ArrayList;
import java.util.List;

/**
 * this class maintains information from select clause.
 */
public final class SelectComponent {


    private List<ResultColumn> resultColumns = new ArrayList<>();


    private boolean hasAggregationFunction = false;
    private boolean hasMoreAggregationFunction = false;
    private boolean hasStarQuery = false;
    private boolean hasCommonQuery = false;


    public void addResultColumn(ResultColumn resultColumn) {
        Expression expression = resultColumn.getExpression();
        if (expression instanceof FunctionExpression) {
            String functionName = ((FunctionExpression) expression).getFunctionName();
            if (SQLConstant.getNativeFunctionNames().contains(functionName.toLowerCase())) {
                if (hasAggregationFunction) {
                    hasMoreAggregationFunction = true;
                } else {
                    hasAggregationFunction = true;
                }
            }
        }
        if (expression instanceof NodeExpression) {
            if (((NodeExpression) expression).getName().equals(SQLConstant.STAR)) {
                hasStarQuery = true;
            }
            hasCommonQuery = true;
        }
        resultColumns.add(resultColumn);
    }

    public void setResultColumns(List<ResultColumn> resultColumns) {
        this.resultColumns = resultColumns;
    }

    public List<ResultColumn> getResultColumns() {
        return resultColumns;
    }


    public boolean isHasAggregationFunction() {
        return hasAggregationFunction;
    }

    public boolean isHasMoreAggregationFunction() {
        return hasMoreAggregationFunction;
    }

    public boolean isHasStarQuery() {
        return hasStarQuery;
    }

    public boolean isHasCommonQuery() {
        return hasCommonQuery;
    }
}
