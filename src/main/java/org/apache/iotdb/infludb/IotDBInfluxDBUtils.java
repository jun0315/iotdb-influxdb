package org.apache.iotdb.infludb;

import kotlin.text.StringsKt;
import org.apache.iotdb.infludb.influxql.Condition;
import org.apache.iotdb.infludb.influxql.Token;
import org.apache.iotdb.infludb.influxql.expr.BinaryExpr;
import org.apache.iotdb.infludb.influxql.expr.Expr;
import org.apache.iotdb.infludb.influxql.expr.ParenExpr;
import org.apache.iotdb.infludb.influxql.expr.VarRef;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.Field;
import org.influxdb.dto.QueryResult;

import java.util.ArrayList;
import java.util.Arrays;
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

    //取交集
    public static QueryResult andQueryResultProcess(QueryResult queryResult1, QueryResult queryResult2) {
        if (!checkSameQueryResult(queryResult1, queryResult2)) {
            System.out.println("QueryResult1 and QueryResult2 is not same attribute");
            return queryResult1;
        }
        List<List<Object>> values1 = queryResult1.getResults().get(0).getSeries().get(0).getValues();
        List<List<Object>> values2 = queryResult2.getResults().get(0).getSeries().get(0).getValues();
        List<List<Object>> sameValues = new ArrayList<>();
        for (List<Object> value1 : values1) {
            for (List<Object> value2 : values2) {
                boolean allEqual = true;
                for (int t = 0; t < value1.size(); t++) {
                    //如果有不相等的话，那么跳过当前j
                    if (!value1.get(t).equals(value2.get(t))) {
                        allEqual = false;
                        break;
                    }
                }
                //此时匹配完，如果完全相等
                if (allEqual) {
                    sameValues.add(value1);
                }
            }
        }
        updateQueryResultValue(queryResult1, sameValues);
        return queryResult1;
    }


    //取并集
    public static QueryResult orQueryResultProcess(QueryResult queryResult1, QueryResult queryResult2) {
        if (!checkSameQueryResult(queryResult1, queryResult2)) {
            System.out.println("QueryResult1 and QueryResult2 is not same attribute");
            return queryResult1;
        }
        List<List<Object>> values1 = queryResult1.getResults().get(0).getSeries().get(0).getValues();
        List<List<Object>> values2 = queryResult2.getResults().get(0).getSeries().get(0).getValues();
        List<List<Object>> notSameValuesInValues1 = new ArrayList<>();
        for (List<Object> value1 : values1) {
            boolean allNotEqual = true;
            for (List<Object> value2 : values2) {
                boolean notEqual = false;
                for (int t = 0; t < value1.size(); t++) {
                    //如果有不相等的话，那么跳过当前j
                    if (!value1.get(t).equals(value2.get(t))) {
                        notEqual = true;
                        break;
                    }
                }
                if (!notEqual) {
                    allNotEqual = false;
                    break;
                }
            }
            if (allNotEqual) {
                notSameValuesInValues1.add(value1);
            }
        }
        //values2加上 不相同的valueList
        values2.add(List.of(notSameValuesInValues1));
        updateQueryResultValue(queryResult1, values2);
        return queryResult1;
    }

    //取并集
    public static QueryResult addQueryResultProcess(QueryResult queryResult1, QueryResult queryResult2) {
        if (!checkSameQueryResult(queryResult1, queryResult2)) {
            System.out.println("QueryResult1 and QueryResult2 is not same attribute");
            return queryResult1;
        }
        List<List<Object>> values1 = queryResult1.getResults().get(0).getSeries().get(0).getValues();
        List<List<Object>> values2 = queryResult2.getResults().get(0).getSeries().get(0).getValues();
        //values相加
        values1.add(List.of(values2));
        updateQueryResultValue(queryResult1, values2);
        return queryResult1;
    }

    private static void updateQueryResultValue(QueryResult queryResult, List<List<Object>> updateValues) {
        List<QueryResult.Result> results = queryResult.getResults();
        QueryResult.Result result = results.get(0);
        List<QueryResult.Series> series = results.get(0).getSeries();
        QueryResult.Series serie = series.get(0);

        serie.setValues(updateValues);
        series.set(0, serie);
        result.setSeries(series);
        results.set(0, result);
    }

    private static boolean checkSameQueryResult(QueryResult queryResult1, QueryResult queryResult2) {
        return queryResult1.getResults().get(0).getSeries().get(0).getName().
                equals(queryResult2.getResults().get(0).getSeries().get(0).getName()) &&
                checkSameStringList(queryResult1.getResults().get(0).getSeries().get(0).getColumns(),
                        queryResult2.getResults().get(0).getSeries().get(0).getColumns());
    }

    private static boolean checkSameStringList(List<String> list1, List<String> list2) {
        if (list1.size() != list2.size()) {
            return false;
        } else {
            for (int i = 0; i < list1.size(); i++) {
                if (!list1.get(i).equals(list2.get(i))) {
                    return false;
                }
            }
        }
        return true;
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

    //截取最后一个点
    public static String getFiledByPath(String path) {
        String[] tmpList = path.split("\\.");
        return tmpList[tmpList.length - 1];
    }

    public static ArrayList<Integer> getSamePathForList(List<String> columnNames) {
        ArrayList<Integer> list = new ArrayList<>();
        //记录上一个结果么，用来判断是否和当前重复
        String lastPath = null;
        for (int i = 1; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            String path = columnName.substring(0, columnName.lastIndexOf("."));
            if (i == 1) {
                lastPath = path;
            } else {
                if (!lastPath.equals(path)) {
                    list.add(i - 1);
                    lastPath = path;
                }
            }
        }
        list.add(columnNames.size() - 1);
        return list;
    }

    public static Object iotdbFiledCvt(Field field) {
        if (field.getDataType() == null) {
            return null;
        }
        switch (field.getDataType()) {
            case TEXT:
                return field.getStringValue();
            case INT64:
                return field.getLongV();
            case INT32:
                return field.getIntV();
            case DOUBLE:
                return field.getDoubleV();
            case FLOAT:
                return field.getFloatV();
            case BOOLEAN:
                return field.getBoolV();
            default:
                return null;
        }
    }
}
