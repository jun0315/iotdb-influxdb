package org.apache.iotdb.infludb.qp.logical.aggregation;

public class AggregationValue {
    private Object value;
    private int timestamp;

    public AggregationValue(Object value, int timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public int getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }
}
