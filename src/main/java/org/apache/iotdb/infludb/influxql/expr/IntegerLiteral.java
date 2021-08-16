package org.apache.iotdb.infludb.influxql.expr;

public class IntegerLiteral implements Expr, Literal {
    public int Val;

    public IntegerLiteral(int val) {
        Val = val;
    }

    public String toString() {
        return String.valueOf(Val);
    }
}
