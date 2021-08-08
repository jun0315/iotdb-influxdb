package org.apache.iotdb.infludb.influxql.expr;

public class ParenExpr implements Expr {
    public ParenExpr(Expr expr) {
        this.Expr = expr;
    }

    public Expr Expr;

    public ParenExpr() {
    }
}
