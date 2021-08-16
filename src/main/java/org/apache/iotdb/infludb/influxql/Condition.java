package org.apache.iotdb.infludb.influxql;

import org.apache.iotdb.infludb.influxql.expr.Literal;

public class Condition {
    //当前查询过滤条件的var
    public String Value;
    public Token Token;
    //当前查询过滤条件的实际值
    public String Literal;

    public Condition(String value, Token token, String literal) {
        Value = value;
        Token = token;
        Literal = literal;
    }

    public Condition() {
    }

    public void setValue(String value) {
        Value = value;
    }

    public void setToken(org.apache.iotdb.infludb.influxql.Token token) {
        Token = token;
    }

    public void setLiteral(String literal) {
        Literal = literal;
    }

    public String getValue() {
        return Value;
    }

    public org.apache.iotdb.infludb.influxql.Token getToken() {
        return Token;
    }

    public String getLiteral() {
        return Literal;
    }
}
