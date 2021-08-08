package org.apache.iotdb.infludb.influxql.expr;

public class NumberLiteral implements Expr, Literal {
    public double Val;

    public NumberLiteral(double val) {
        Val = val;
    }

    public NumberLiteral() {
    }
}
