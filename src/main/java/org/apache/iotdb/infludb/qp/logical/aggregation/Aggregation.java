package org.apache.iotdb.infludb.qp.logical.aggregation;

import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.List;

public interface Aggregation {

    public void updateValue(AggregationValue aggregationValue);


    public AggregationValue calculate();
    public List<Expression> getExpressions();
}
