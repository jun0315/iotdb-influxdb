package org.apache.iotdb.infludb.qp.logical.aggregation;

import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.List;

public class MinAggregation implements Aggregation {
    private Double doubleValue = Double.MAX_VALUE;
    private String stringValue = null;
    private boolean isNumber = false;
    private boolean isString = false;
    private Long timestamp;

    private List<Expression> expressionList;

    public MinAggregation(List<Expression> expressionList) {
        this.expressionList = expressionList;
    }

    public MinAggregation() {
    }

    @Override
    public void updateValue(AggregationValue aggregationValue) {
        Object value = aggregationValue.getValue();
        Long timestamp = aggregationValue.getTimestamp();
        if (value instanceof Number) {
            if (!isNumber) {
                isNumber = true;
            }
            Double tmpValue = (Double) value;
            if (tmpValue <= this.doubleValue) {
                doubleValue = tmpValue;
                this.timestamp = timestamp;
            }
        } else if (value instanceof String) {
            String tmpValue = (String) value;
            if (!isString) {
                isString = true;
                stringValue = tmpValue;
                this.timestamp = timestamp;
            } else {
                if (tmpValue.compareTo(this.stringValue) <= 0) {
                    stringValue = tmpValue;
                    this.timestamp = timestamp;
                }

            }
        }
    }

    @Override
    public AggregationValue calculate() {
        if (!isString && !isNumber) {
            throw new IllegalArgumentException("not valid type");
        } else if (isString) {
            return new AggregationValue(stringValue, timestamp);
        } else {
            return new AggregationValue(doubleValue, timestamp);
        }
    }

    @Override
    public List<Expression> getExpressions() {
        return this.expressionList;
    }
}
