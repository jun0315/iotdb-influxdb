package org.apache.iotdb.infludb.qp.logical.aggregation;

import org.apache.iotdb.infludb.qp.utils.TypeUtil;
import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.List;

public class MeanAggregation implements Aggregation {
    public Double value;
    public int sum;
    private List<Expression> expressionList;

    public MeanAggregation(List<Expression> expressionList) {
        this.expressionList = expressionList;
    }

    public MeanAggregation() {
    }


    @Override
    public void updateValue(AggregationValue aggregationValue) {
        Object value = aggregationValue.getValue();
        if (TypeUtil.checkDecimal(value)) {
            sum++;
            this.value += (Double) value;
        } else {
            throw new IllegalArgumentException("not valid type");
        }

    }

    @Override
    public AggregationValue calculate() {
        return new AggregationValue(String.valueOf(this.value / sum), 0);
    }

    @Override
    public List<Expression> getExpressions() {
        return this.expressionList;
    }

}
