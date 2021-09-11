package org.apache.iotdb.infludb.qp.logical.aggregation;

import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.List;

public class AggregationFactory {
    public static Aggregation generateAggregation(String name, List<Expression> expressionList) {
        return switch (name) {
            case "max" -> new MaxAggregation(expressionList);
            case "min" -> new MinAggregation(expressionList);
            case "mean" -> new MeanAggregation(expressionList);
            default -> throw new IllegalArgumentException("not support aggregation name:" + name);
        };
    }
}
