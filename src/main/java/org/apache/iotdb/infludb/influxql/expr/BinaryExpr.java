package org.apache.iotdb.infludb.influxql.expr;

import org.apache.iotdb.infludb.influxql.Token;

public class BinaryExpr implements Expr{
    public Token Op;
    public Expr LHS;
    public Expr RHS;

    public BinaryExpr(Token op, Expr LHS, Expr RHS) {
        Op = op;
        this.LHS = LHS;
        this.RHS = RHS;
    }

    public BinaryExpr() {
    }
}
