package org.apache.iotdb.infludb.qp.logical.function;

import org.apache.iotdb.infludb.qp.constant.SQLConstant;
import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.List;

public class FunctionFactory {
    public static Function generateFunction(String name, List<Expression> expressionList) {
        return switch (name) {
            case SQLConstant.MAX -> new MaxFunction(expressionList);
            case SQLConstant.MIN -> new MinFunction(expressionList);
            case SQLConstant.MEAN -> new MeanFunction(expressionList);
            case SQLConstant.LAST -> new LastFunction(expressionList);
            case SQLConstant.FIRST -> new FirstFunction(expressionList);
            case SQLConstant.COUNT -> new CountFunction(expressionList);
            case SQLConstant.MEDIAN -> new MedianFunction(expressionList);
            case SQLConstant.MODE -> new ModeFunction(expressionList);
            case SQLConstant.SPREAD -> new SpreadFunction(expressionList);
            case SQLConstant.STDDEV -> new StddevFunction(expressionList);
            case SQLConstant.SUM-> new SumFunction(expressionList);
            default -> throw new IllegalArgumentException("not support aggregation name:" + name);
        };
    }
}
