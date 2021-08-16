package org.apache.iotdb.infludb.influxql.expr;

public class StringLiteral implements Expr, Literal {
    public String Val;

    public StringLiteral(String val) {
        Val = val;
    }

    public String toString() {
        return "\"" + Val + "\"";
    }

}
