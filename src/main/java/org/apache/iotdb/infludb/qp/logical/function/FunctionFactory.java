package org.apache.iotdb.infludb.qp.logical.function;

import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.List;

public class FunctionFactory {
    public static Function generateFunction(String name, List<Expression> expressionList) {
        return switch (name) {
            case "max" -> new MaxFunction(expressionList);
            case "min" -> new MinFunction(expressionList);
            case "mean" -> new MeanFunction(expressionList);
            case "last" -> new LastFunction(expressionList);
            case "first" -> new FirstFunction(expressionList);
            default -> throw new IllegalArgumentException("not support aggregation name:" + name);
        };
    }
}
