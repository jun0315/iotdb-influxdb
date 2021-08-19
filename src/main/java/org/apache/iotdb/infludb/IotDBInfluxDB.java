package org.apache.iotdb.infludb;

import org.apache.iotdb.infludb.influxql.expr.*;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.influxdb.dto.*;
import org.apache.iotdb.infludb.influxql.*;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class IotDBInfluxDB {

    private static Session session;
    //当前influxdb选择的database
    private String database;
    //当前influxdb选择的measurement
    private String measurement;
    //当前database下的所有measurement对应的tag列表及顺序
    //TODO 当前不考虑分布式的情况,假定写入都是由该实例执行的
    private Map<String, Map<String, Integer>> measurementTagOrder = new HashMap<>();
    //当前measurement下的tag列表及顺序
    private Map<String, Integer> tagOrders;

    //当前measurement下的field列表及指定规则的顺序
    private Map<String, Integer> fieldOrders;
    //反转map
    private Map<Integer, String> fieldOrdersReversed;

    //占位符
    private final String placeholder = "PH";

    //构造函数
    public IotDBInfluxDB(String url, String userName, String password) {
        try {
            URI uri = new URI(url);
            new IotDBInfluxDB(uri.getHost(), uri.getPort(), userName, password);
        } catch (URISyntaxException | IoTDBConnectionException e) {
            e.printStackTrace();
        }
    }

    //构造函数
    public IotDBInfluxDB(String host, int rpcPort, String userName, String password) throws IoTDBConnectionException {
        session = new Session(host, rpcPort, userName, password);
        session.open(false);

        session.setFetchSize(10000);
    }

    //写入函数
    public void write(Point point) throws IoTDBConnectionException, StatementExecutionException {
        String measurement = null;
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();
        Long time = null;
        Field[] reflectFields = point.getClass().getDeclaredFields();
        //利用反射获取influxdb中point的属性
        for (Field reflectField : reflectFields) {
            reflectField.setAccessible(true);
            try {
                if (reflectField.getType().getName().equalsIgnoreCase("java.util.Map") && reflectField.getName().equalsIgnoreCase("fields")) {
                    fields = (Map<String, Object>) reflectField.get(point);
                }
                if (reflectField.getType().getName().equalsIgnoreCase("java.util.Map") && reflectField.getName().equalsIgnoreCase("tags")) {
                    tags = (Map<String, String>) reflectField.get(point);
                }
                if (reflectField.getType().getName().equalsIgnoreCase("java.lang.String") && reflectField.getName().equalsIgnoreCase("measurement")) {
                    measurement = (String) reflectField.get(point);
                }
                if (reflectField.getType().getName().equalsIgnoreCase("java.lang.Number") && reflectField.getName().equalsIgnoreCase("time")) {
                    time = (Long) reflectField.get(point);
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        //设置为当前时间
        if (time == null) {
            time = System.currentTimeMillis();
        }
        tagOrders = measurementTagOrder.get(measurement);
        if (tagOrders == null) {
            tagOrders = new HashMap<>();
        }
        int measurementTagNum = tagOrders.size();
        //当前插入时实际tag的数量
        Map<Integer, String> realTagOrders = new HashMap<>();
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            if (tagOrders.containsKey(entry.getKey())) {
                realTagOrders.put(tagOrders.get(entry.getKey()), entry.getKey());
            } else {
                measurementTagNum++;
                updateNewTag(measurement, entry.getKey(), measurementTagNum);
                realTagOrders.put(measurementTagNum, entry.getKey());
                tagOrders.put(entry.getKey(), measurementTagNum);
            }
        }
        //更新内存中map
        measurementTagOrder.put(database, tagOrders);
        StringBuilder path = new StringBuilder("root." + database + "." + measurement);
        for (int i = 1; i <= measurementTagNum; i++) {
            if (realTagOrders.containsKey(i)) {
                path.append(".").append(tags.get(realTagOrders.get(i)));
            } else {
                path.append("." + placeholder);
            }
        }

        List<String> measurements = new ArrayList<>();
        List<TSDataType> types = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            measurements.add(entry.getKey());
            var value = entry.getValue();
            if (value instanceof String) {
                types.add(TSDataType.TEXT);
            } else if (value instanceof Integer) {
                types.add(TSDataType.INT32);
            } else if (value instanceof Double) {
                types.add(TSDataType.DOUBLE);
            } else {
                System.err.printf("can't solve type:%s", entry.getValue().getClass());
            }
            values.add(value);
        }
        session.insertRecord(String.valueOf(path), time, measurements, types, values);
    }

    //插入时出现新的tag，把新的tag更新到内存和数据库中
    public void updateNewTag(String measurement, String tag, int order) throws IoTDBConnectionException, StatementExecutionException {
        List<String> measurements = new ArrayList<>();
        List<TSDataType> types = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        measurements.add("database_name");
        measurements.add("measurement_name");
        measurements.add("tag_name");
        measurements.add("tag_order");
        types.add(TSDataType.TEXT);
        types.add(TSDataType.TEXT);
        types.add(TSDataType.TEXT);
        types.add(TSDataType.INT32);
        values.add(database);
        values.add(measurement);
        values.add(tag);
        values.add(order);
        session.insertRecord("root.TAG_INFO", System.currentTimeMillis(), measurements, types, values);
    }

    //查询函数
    public QueryResult query(Query query) {
        return null;
    }

    public QueryResult query() throws Exception {
        //sql
        //String sql = "select * from student where (name= 'xie' and sex='m') or (sex= 'fm' and age=92)";

        //构造sql参数
        String measurement = "student";
        BinaryExpr binaryExpr = new BinaryExpr();
        binaryExpr.Op = Token.OR;
        binaryExpr.LHS = new ParenExpr(new BinaryExpr(
                Token.AND,
                new BinaryExpr(Token.EQ, new VarRef("name", DataType.Unknown), new StringLiteral("qi")),
                new BinaryExpr(Token.EQ, new VarRef("sex", DataType.Unknown), new StringLiteral("fm"))
        ));
        binaryExpr.RHS = new ParenExpr(new BinaryExpr(
                Token.AND,
                new BinaryExpr(Token.EQ, new VarRef("sex", DataType.Unknown), new StringLiteral("fm")),
                new BinaryExpr(Token.EQ, new VarRef("age", DataType.Unknown), new IntegerLiteral(92))
        ));
        changeMeasurement(measurement);
        updateFiledOrders();

        queryExpr(binaryExpr);
        return null;
    }

    //更新当前measure的所有的field列表及指定顺序
    private void updateFiledOrders() throws IoTDBConnectionException, StatementExecutionException {
        //先初始化
        fieldOrders = new HashMap<>();
        fieldOrdersReversed = new HashMap<>();
        String showTimeseriesSql = "show timeseries root." + database + '.' + measurement;
        SessionDataSet result = session.executeQueryStatement(showTimeseriesSql);
        int fieldNums = 0;
        int tagOrderNums = tagOrders.size();
        while (result.hasNext()) {
            List<org.apache.iotdb.tsfile.read.common.Field> fields = result.next().getFields();
            String filed = IotDBInfluxDBUtils.getFiledByPath(fields.get(0).getStringValue());
            if (!fieldOrders.containsKey(filed)) {
                //field对应的顺序是1+tagNum （第一个是时间戳，接着是所有的tag，最后是所有的field）
                fieldOrders.put(filed, tagOrderNums + fieldNums + 1);
                fieldOrdersReversed.put(tagOrderNums + fieldNums + 1, filed);
                fieldNums++;
            }
        }
    }

    //更改当前的measurement
    private void changeMeasurement(String measurement) {
        if (!measurement.equals(this.measurement)) {
            this.measurement = measurement;
            tagOrders = measurementTagOrder.get(measurement);
            if (tagOrders == null) {
                tagOrders = new HashMap<>();
            }
        }
    }

    //创建database
    public void createDatabase(String name) {
        IotDBInfluxDBUtils.checkNonEmptyString(name, "name");
        try {
            session.setStorageGroup("root." + name);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (e instanceof StatementExecutionException statementExecutionException && statementExecutionException.getStatusCode() == 300) {
                //当前database已经被创建过
                System.out.println(statementExecutionException.getMessage());
            } else {
                e.printStackTrace();
            }
        }
    }

    //删除database
    public void deleteDatabase(String name) {
        try {
            session.deleteStorageGroup("root." + name);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        }
    }

    //设置当前的database
    public void setDatabase(String database) {
        if (!database.equals(this.database)) {
            updateDatabase(database);
            this.database = database;
        }
    }

    //当database发生改变时，更新database相关信息
    private void updateDatabase(String database) {
        try {
            //TODO 为用户提供初始化sh，将root.TAG_INFO相关表结构创建出来 set storage group to root.TAG_INFO
            var result = session.executeQueryStatement("select * from root.TAG_INFO where database_name=" + String.format("\"%s\"", database));
            Map<String, Integer> tagOrder = new HashMap<>();
            String measurementName = null;
            while (result.hasNext()) {
                var fields = result.next().getFields();
                String tmpMeasurementName = fields.get(1).getStringValue();
                if (measurementName == null) {
                    //首次获取到measurementName
                    measurementName = tmpMeasurementName;
                } else {
                    //不相等的话，则是遇到了新的measurement
                    if (!tmpMeasurementName.equals(measurementName)) {
                        //将当前measurement的tags加入其中
                        measurementTagOrder.put(measurementName, tagOrder);
                        tagOrder = new HashMap<>();
                    }
                }
                tagOrder.put(fields.get(2).getStringValue(), fields.get(3).getIntV());
            }
            //最后一个measurement，将当前measurement的tags加入其中
            measurementTagOrder.put(measurementName, tagOrder);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //通过条件获取查询结果
    private QueryResult queryByConditions(List<Condition> conditions) throws IoTDBConnectionException, StatementExecutionException {
        //用来存储符合tag的实际顺序
        Map<Integer, Condition> realTagOrders = new HashMap<>();
        //存储属于field的conditions列表
        List<Condition> fieldConditions = new ArrayList<>();
        //当前measurement中tag的数目
        int measurementTagNum = tagOrders.size();
        //当前查询条件中的最大tag数目
        int currentQueryMaxTagNum = 0;
        for (Condition condition : conditions) {
            //当前条件是处于tag中
            if (tagOrders.containsKey(condition.getValue())) {
                int curOrder = tagOrders.get(condition.getValue());
                //将其放入符合tag的map中
                realTagOrders.put(curOrder, condition);
                //更新当前查询条件的最大tag顺序
                currentQueryMaxTagNum = Math.max(currentQueryMaxTagNum, curOrder);
            } else {
                fieldConditions.add(condition);
            }
        }
        //构造实际的查询路径
        StringBuilder curQueryPath = new StringBuilder("root." + database + "." + measurement);
        //从1遍历到当前查询条件的最大数目
        for (int i = 1; i <= currentQueryMaxTagNum; i++) {
            if (realTagOrders.containsKey(i)) {
                //由于是path中的value，因此需要把首尾的引号去除
                curQueryPath.append(".").append(IotDBInfluxDBUtils.removeQuotation(realTagOrders.get(i).getLiteral()));
            } else {
                curQueryPath.append(".").append("*");
            }
        }
        //构造实际的查询条件
        StringBuilder realIotDBCondition = new StringBuilder();
        for (int i = 0; i < fieldConditions.size(); i++) {
            Condition condition = fieldConditions.get(i);
            if (i != 0) {
                realIotDBCondition.append(" and ");
            }
            realIotDBCondition.append(condition.getValue()).append(" ")
                    .append(condition.getToken().getOperate()).append(" ")
                    .append(condition.getLiteral());
        }
        //实际的查询sql语句
        String realQuerySql;
        //没有iotdb的where过滤条件
        QueryResult queryResult = null;
        if (realIotDBCondition.isEmpty()) {
            realQuerySql = ("select * from " + curQueryPath);
            SessionDataSet sessionDataSet = session.executeQueryStatement(realQuerySql);
            queryResult = iotdbResultCvtToInfluxdbResult(sessionDataSet);
            System.out.println(sessionDataSet.toString());
        } else {
            //有了过滤条件，只能多次遍历
            QueryResult lastQueryResult = null;
            for (int i = currentQueryMaxTagNum; i <= measurementTagNum; i++) {
                if (i != currentQueryMaxTagNum) {
                    curQueryPath.append(".*");
                }
                realQuerySql = ("select * from " + curQueryPath + " where " + realIotDBCondition);
                SessionDataSet sessionDataSet = null;
                try {
                    sessionDataSet = session.executeQueryStatement(realQuerySql);
                } catch (StatementExecutionException e) {
                    if (e.getStatusCode() == 411) {
                        //where的timeseries没有匹配的，会抛出411的错误，将其拦截打印
                        System.out.println(e.getMessage());
                    } else {
                        throw e;
                    }
                }
                //暂时的转换结果
                QueryResult tmpQueryResult = iotdbResultCvtToInfluxdbResult(sessionDataSet);
                //如果是第一次，则直接赋值，不需要or操作
                if (i == currentQueryMaxTagNum) {
                    lastQueryResult = tmpQueryResult;
                } else {
                    //进行add操作
                    lastQueryResult = IotDBInfluxDBUtils.addQueryResultProcess(lastQueryResult, tmpQueryResult);
                }
            }
            queryResult = lastQueryResult;
        }
        return queryResult;
    }

    //将iotdb的查询结果转换为influxdb的查询结果
    private QueryResult iotdbResultCvtToInfluxdbResult(SessionDataSet sessionDataSet) throws IoTDBConnectionException, StatementExecutionException {
        if (sessionDataSet == null) {
            //TODO return null QueryResult;
            return null;
        }
        List<String> iotdbResultColumn = sessionDataSet.getColumnNames();
        ArrayList<Integer> samePath = IotDBInfluxDBUtils.getSamePathForList(iotdbResultColumn.subList(1, iotdbResultColumn.size()));
        //生成series
        QueryResult.Series series = new QueryResult.Series();
        series.setName(measurement);
        //获取tag的反向map
        Map<Integer, String> tagOrderReversed = tagOrders.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        int tagSize = tagOrderReversed.size();
        ArrayList<String> tagList = new ArrayList<>();
        for (int i = 1; i <= tagSize; i++) {
            tagList.add(tagOrderReversed.get(i));
        }

        ArrayList<String> fieldList = new ArrayList<>();
        for (int i = 1 + tagSize; i <= 1 + tagSize + fieldOrders.size(); i++) {
            fieldList.add(fieldOrdersReversed.get(i));
        }
        ArrayList<String> columns = new ArrayList<>();
        columns.add("time");
        columns.addAll(tagList);
        columns.addAll(fieldList);
        //把columns插入series中
        series.setColumns(columns);

        List<List<Object>> values = new ArrayList<>();
        while (sessionDataSet.hasNext()) {
            Object[] value = new Object[columns.size()];

            RowRecord record = sessionDataSet.next();
            List<org.apache.iotdb.tsfile.read.common.Field> fields = record.getFields();
            long timestamp = record.getTimestamp();
            //判断该path是否所有的值都为null
            boolean allNull = true;
            //记录sameList的当前下标
            int sameListIndex = 0;
            for (int i = 0; i < fields.size(); i++) {
                Object o = IotDBInfluxDBUtils.iotdbFiledCvt(fields.get(i));
                if (o != null) {
                    if (allNull) {
                        allNull = false;
                    }
                    //将filed的值插入其中
                    value[fieldOrders.get(IotDBInfluxDBUtils.getFiledByPath(iotdbResultColumn.get(i + 1)))] = o;
                }
                //该相同的path已经遍历完成
                if (i == samePath.get(sameListIndex)) {
                    //如果数据中有非null，则插入实际的数据中，否则直接跳过
                    if (!allNull) {
                        //先把时间插入value中
                        value[0] = timestamp;
                        //再把该path中的tag插入value中国
                        //加1，第零列是time
                        String tmpPathName = iotdbResultColumn.get(i + 1);
                        String[] tmpTags = tmpPathName.split("\\.");
                        for (int j = 3; i < tmpTags.length - 1; i++) {
                            if (!tmpTags[j].equals(placeholder)) {
                                //放入指定的序列中
                                value[j - 2] = tmpTags[j];
                            }
                        }
                    }
                    //插入实际的value
                    values.add(Arrays.asList(value));
                    //重制value
                    value = new Object[columns.size()];
                }
            }
        }
        series.setValues(values);

        QueryResult queryResult = new QueryResult();
        QueryResult.Result result = new QueryResult.Result();
        result.setSeries(List.of(series));
        queryResult.setResults(List.of(result));

        return queryResult;
    }


    public QueryResult queryExpr(Expr expr) throws Exception {
        if (expr instanceof BinaryExpr binaryExpr) {
            if (binaryExpr.Op == Token.OR) {
                return IotDBInfluxDBUtils.orQueryResultProcess(queryExpr(binaryExpr.LHS), queryExpr(binaryExpr.RHS));
            } else if (binaryExpr.Op == Token.AND) {
                if (IotDBInfluxDBUtils.canMergeExpr(binaryExpr.LHS) && IotDBInfluxDBUtils.canMergeExpr(binaryExpr.RHS)) {
                    List<Condition> conditions1 = IotDBInfluxDBUtils.getConditionsByExpr(binaryExpr.LHS);
                    List<Condition> conditions2 = IotDBInfluxDBUtils.getConditionsByExpr(binaryExpr.RHS);
                    assert conditions1 != null;
                    assert conditions2 != null;
                    conditions1.addAll(conditions2);
                    return queryByConditions(conditions1);
                } else {
                    return IotDBInfluxDBUtils.andQueryResultProcess(queryExpr(binaryExpr.LHS), queryExpr(binaryExpr.RHS));
                }
            } else {
                List<Condition> conditions = new ArrayList<>();
                conditions.add(IotDBInfluxDBUtils.getConditionForSingleExpr(binaryExpr));
                return queryByConditions(conditions);
            }
        } else if (expr instanceof ParenExpr parenExpr) {
            return queryExpr(parenExpr.Expr);
        } else {
            throw new Exception("don't allow type:" + expr.toString());
        }
    }

    public static void main(String[] args) throws Exception {
        //初始化
        var iotDBInfluxDB = new IotDBInfluxDB("http://127.0.0.1:6667", "root", "root");
        //创建database
        iotDBInfluxDB.createDatabase("database");
        //设置database
        iotDBInfluxDB.setDatabase("database");
        //构造influxdb的插入build参数
        Point.Builder builder = Point.measurement("student");
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();
        tags.put("name", "xie");
        tags.put("stress", "huai");
        tags.put("sex", "m");
        fields.put("score", "87");
        fields.put("tel", 110);
        builder.tag(tags);
        builder.fields(fields);
        builder.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        Point point = builder.build();
        //build构造完成，开始write
//        iotDBInfluxDB.write(point);

        //开始查询
        var result = iotDBInfluxDB.query();
    }
}
