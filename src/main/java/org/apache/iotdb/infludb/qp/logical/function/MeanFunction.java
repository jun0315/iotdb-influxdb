package org.apache.iotdb.infludb.qp.logical.function;

import org.apache.iotdb.infludb.qp.utils.TypeUtil;
import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.List;

public class MeanFunction extends Aggregate {
    public Double value = (double) 0;
    public int sum;

    public MeanFunction(List<Expression> expressionList) {
        super(expressionList);
    }

    public MeanFunction() {
    }


    @Override
    public void updateValue(FunctionValue functionValue) {
        Object value = functionValue.getValue();
        if (TypeUtil.checkDecimal(value)) {
            sum++;
            this.value += ((Number) value).doubleValue();
        } else {
            throw new IllegalArgumentException("mean not valid type");
        }

    }

    @Override
    public FunctionValue calculate() {
        return new FunctionValue(String.valueOf(this.value / sum), 0L);
    }

}
