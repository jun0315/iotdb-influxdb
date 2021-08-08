package org.apache.iotdb.infludb.influxql.expr;

import org.apache.iotdb.infludb.influxql.DataType;

public class VarRef implements Expr{

    public String Val;
    public DataType Type;

    public VarRef(String val, DataType type) {
        Val = val;
        Type = type;
    }
}
