import org.apache.iotdb.infludb.influxql.Condition;
import org.apache.iotdb.infludb.influxql.DataType;
import org.apache.iotdb.infludb.influxql.Token;
import org.apache.iotdb.infludb.influxql.expr.*;
import org.apache.iotdb.session.SessionDataSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class trys {
    public static void main(String[] args) {
        String sql = "select * from cpu where host = 'serverA' or host='serverB' and (regions = 'us' or value=0.77)";

        BinaryExpr binaryExpr = new BinaryExpr();
        binaryExpr.Op = Token.OR;
        binaryExpr.LHS = new BinaryExpr(Token.EQ, new VarRef("host", DataType.Unknown), new StringLiteral("serverA"));
        binaryExpr.RHS = new BinaryExpr(Token.AND,
                new BinaryExpr(Token.EQ, new VarRef("host", DataType.Unknown), new StringLiteral("serverB")),
                new ParenExpr(new BinaryExpr(Token.OR,
                        new BinaryExpr(Token.EQ, new VarRef("regions", DataType.Unknown), new StringLiteral("us")),
                        new BinaryExpr(Token.EQ, new VarRef("value", DataType.Unknown), new NumberLiteral(0.77)))));
        System.out.println(sql);
    }

    //并集
    private SessionDataSet orProcess(SessionDataSet dataSet1, SessionDataSet dataSet2) {
        return dataSet1;
    }

    //交集
    private SessionDataSet andProcess(SessionDataSet dataSet1, SessionDataSet dataSet2) {
        return dataSet1;
    }

    //判断该expr是否有or的操作,是否可以合并查询
    private boolean canMergeExpr(Expr expr) {
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

    private Condition getConditionForSingleExpr(BinaryExpr binaryExpr) {
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

    //如果进入这个函数，说明一定是可以合并的语法树，不存在or的情况
    private List<Condition> getConditionsByExpr(Expr expr) {
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


    //通过条件获取查询结果
    private SessionDataSet queryByConditions(List<Condition> conditions) {
        return null;
    }


    public SessionDataSet queryExpr(Expr expr) throws Exception {
        if (expr instanceof BinaryExpr binaryExpr) {
            if (binaryExpr.Op == Token.OR) {
                return orProcess(queryExpr(binaryExpr.LHS), queryExpr(binaryExpr.RHS));
            } else if (binaryExpr.Op == Token.AND) {
                if (canMergeExpr(binaryExpr.LHS) && canMergeExpr(binaryExpr.RHS)) {
                    List<Condition> conditions1 = getConditionsByExpr(binaryExpr.LHS);
                    List<Condition> conditions2 = getConditionsByExpr(binaryExpr.RHS);
                    assert conditions1 != null;
                    assert conditions2 != null;
                    conditions1.addAll(conditions2);
                    return queryByConditions(conditions1);
                } else {
                    return andProcess(queryExpr(binaryExpr.LHS), queryExpr(binaryExpr.RHS));
                }
            } else {
                List<Condition> conditions = new ArrayList<>();
                conditions.add(getConditionForSingleExpr(binaryExpr));
                return queryByConditions(conditions);
            }
        } else if (expr instanceof ParenExpr parenExpr) {
            return queryExpr(parenExpr.Expr);
        } else {
            throw new Exception("don't allow type:" + expr.toString());
        }
    }


}
