package org.apache.iotdb.infludb;

import org.apache.iotdb.infludb.influxql.Condition;
import org.apache.iotdb.infludb.influxql.Token;
import org.apache.iotdb.infludb.influxql.expr.BinaryExpr;
import org.apache.iotdb.infludb.influxql.expr.Expr;
import org.apache.iotdb.infludb.influxql.expr.ParenExpr;
import org.apache.iotdb.infludb.influxql.expr.VarRef;
import org.apache.iotdb.session.SessionDataSet;

import java.util.ArrayList;
import java.util.List;

public final class IotDBInfluxDBUtils {
    //检查当前字段是否为空
    public static String checkNonEmptyString(String string, String name) throws IllegalArgumentException {
        if (string != null && !string.isEmpty()) {
            return string;
        } else {
            throw new IllegalArgumentException("Expecting a non-empty string for " + name);
        }
    }

    //如果当前字符串的第一个和最后一个是引号，则将其去除
    public static String removeQuotation(String str) {
        if (str.charAt(0) == '"' && str.charAt(str.length() - 1) == '"') {
            return str.substring(1, str.length() - 1);
        }
        return str;
    }

    //取并集
    public static SessionDataSet orProcess(SessionDataSet dataSet1, SessionDataSet dataSet2) {
        return dataSet1;
    }

    //取交集
    public static SessionDataSet andProcess(SessionDataSet dataSet1, SessionDataSet dataSet2) {
        return dataSet1;
    }

    //判断该expr是否有or的操作,是否可以合并查询
    public static boolean canMergeExpr(Expr expr) {
        if (expr instanceof BinaryExpr binaryExpr) {
            if (binaryExpr.Op == Token.OR) {
                return false;
            } else if (binaryExpr.Op == Token.AND) {
                return canMergeExpr(binaryExpr.LHS) && canMergeExpr(binaryExpr.RHS);
            } else {
                return true;
            }
        } else if (expr instanceof ParenExpr parenExpr) {
            return canMergeExpr(parenExpr.Expr);
        } else {
            return true;
        }
    }

    //如果进入这个函数，说明一定是可以合并的语法树，不存在or的情况
    public static List<Condition> getConditionsByExpr(Expr expr) {
        if (expr instanceof ParenExpr parenExpr) {
            return getConditionsByExpr(parenExpr.Expr);
        } else if (expr instanceof BinaryExpr binaryExpr) {
            if (binaryExpr.Op == Token.AND) {
                List<Condition> conditions1 = getConditionsByExpr(binaryExpr.LHS);
                List<Condition> conditions2 = getConditionsByExpr(binaryExpr.RHS);
                assert conditions1 != null;
                assert conditions2 != null;
                conditions1.addAll(conditions2);
                return conditions1;
            } else {
                //一定会是非or的情况
                List<Condition> conditions = new ArrayList<>();
                conditions.add(getConditionForSingleExpr(binaryExpr));
                return conditions;
            }
        }
        return null;
    }

    public static Condition getConditionForSingleExpr(BinaryExpr binaryExpr) {
        Condition condition = new Condition();
        condition.setToken(binaryExpr.Op);
        if (binaryExpr.LHS instanceof VarRef varRef) {
            condition.setValue(varRef.Val);
            condition.setLiteral((binaryExpr.RHS).toString());
        } else if (binaryExpr.RHS instanceof VarRef varRef) {
            condition.setValue(varRef.Val);
            condition.setLiteral((binaryExpr.LHS).toString());
        }
        return condition;
    }

}
