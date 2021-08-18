package org.apache.iotdb.infludb;

import org.apache.iotdb.infludb.influxql.expr.*;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.influxdb.InfluxDB;
import org.influxdb.dto.*;
import org.apache.iotdb.infludb.influxql.*;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
        session.insertRecord("root.TAG_INFO_NEW", System.currentTimeMillis(), measurements, types, values);
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
                new BinaryExpr(Token.EQ, new VarRef("name", DataType.Unknown), new StringLiteral("xie")),
                new BinaryExpr(Token.EQ, new VarRef("sex", DataType.Unknown), new StringLiteral("m"))
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
                fieldNums++;
            }
        }
        System.out.println(result);

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
            var result = session.executeQueryStatement("select * from root.TAG_INFO_NEW where database_name=" + String.format("\"%s\"", database));
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
                SessionDataSet sessionDataSet = session.executeQueryStatement(realQuerySql);
                //暂时的转换结果
                QueryResult tmpQueryResult = iotdbResultCvtToInfluxdbResult(sessionDataSet);
                //如果是第一次，则直接赋值，不需要or操作
                if (i == currentQueryMaxTagNum) {
                    lastQueryResult = tmpQueryResult;
                } else {
                    //进行or操作
                    lastQueryResult = IotDBInfluxDBUtils.orProcess(lastQueryResult, tmpQueryResult);
                }
            }
            queryResult = lastQueryResult;
        }
        return queryResult;
    }

    //将iotdb的查询结果转换为influxdb的查询结果
    private QueryResult iotdbResultCvtToInfluxdbResult(SessionDataSet sessionDataSet) {
        return null;
    }


    public QueryResult queryExpr(Expr expr) throws Exception {
        if (expr instanceof BinaryExpr binaryExpr) {
            if (binaryExpr.Op == Token.OR) {
                return IotDBInfluxDBUtils.orProcess(queryExpr(binaryExpr.LHS), queryExpr(binaryExpr.RHS));
            } else if (binaryExpr.Op == Token.AND) {
                if (IotDBInfluxDBUtils.canMergeExpr(binaryExpr.LHS) && IotDBInfluxDBUtils.canMergeExpr(binaryExpr.RHS)) {
                    List<Condition> conditions1 = IotDBInfluxDBUtils.getConditionsByExpr(binaryExpr.LHS);
                    List<Condition> conditions2 = IotDBInfluxDBUtils.getConditionsByExpr(binaryExpr.RHS);
                    assert conditions1 != null;
                    assert conditions2 != null;
                    conditions1.addAll(conditions2);
                    return queryByConditions(conditions1);
                } else {
                    return IotDBInfluxDBUtils.andProcess(queryExpr(binaryExpr.LHS), queryExpr(binaryExpr.RHS));
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
        Map<String, String> tags = new HashMap<String, String>();
        Map<String, Object> fields = new HashMap<String, Object>();
        tags.put("name", "qi");
        tags.put("address", "anhui");
        tags.put("sex", "fm");
        fields.put("score", "97");
        fields.put("age", 92);
        fields.put("num", 3.1);
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
