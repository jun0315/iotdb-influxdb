package org.apache.iotdb.infludb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.*;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class IotDBInfluxDB {
    private InfluxDB influxDB;

    private static Session session;
    private String storageGroup;
    private String database;
    private Map<String, Map<String, Integer>> measurementTagOrder = new HashMap<>();


    private final String placeholder = "PH";

    public IotDBInfluxDB(String url, String userName, String password) {
        try {
            URI uri = new URI(url);
            new IotDBInfluxDB(uri.getHost(), uri.getPort(), userName, password);
        } catch (URISyntaxException | IoTDBConnectionException e) {
            e.printStackTrace();
        }

    }

    public IotDBInfluxDB(String host, int rpcPort, String userName, String password) throws IoTDBConnectionException {
        session = new Session(host, rpcPort, userName, password);
        session.open(false);

        session.setFetchSize(10000);
    }

    public void write(Point point) throws IoTDBConnectionException, StatementExecutionException {
        String measurement = null;
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();
        Long time = null;
        Field[] reflectFields = point.getClass().getDeclaredFields();
        for (Field reflectField : reflectFields) {
            reflectField.setAccessible(true);
            System.out.println(reflectField.getName() + ":" + reflectField.getType().getName());
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
                System.out.println("1");
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        if (time == null) {
            time = System.currentTimeMillis();
        }
        Map<String, Integer> tagOrders = measurementTagOrder.get(database);
        int measurementTagNum = tagOrders.size();
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
                System.err.printf("get solve type:%s", entry.getValue().getClass());
            }
            values.add(value);
        }
        session.insertRecord(String.valueOf(path), time, measurements, types, values);
    }

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

    public QueryResult query(Query query) {
        return null;
    }

    public void createDatabase(String name) {
        IotDBInfluxDBUtils.checkNonEmptyString(name, "name");
        try {
            session.setStorageGroup("root." + name);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        }
    }

    public void deleteDatabase(String name) {
        try {
            session.deleteStorageGroup("root." + name);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        }
    }

    public IotDBInfluxDB setDatabase(String database) {
        this.storageGroup = "root." + database;
        this.database = database;
        try {
            var result = session.executeQueryStatement("select * from root.TAG_INFO where database_name=" + String.format("\"%s\"", database));
            Map<String, Integer> tagOrder = new HashMap<>();
            while (result.hasNext()) {
                var fields = result.next().getFields();
                tagOrder.put(fields.get(2).getStringValue(), (int) fields.get(3).getFloatV());
            }
            measurementTagOrder.put(database, tagOrder);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return this;
    }


    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        var iotDBInfluxDB = new IotDBInfluxDB("http://127.0.0.1:6667", "root", "root");
        iotDBInfluxDB.setDatabase("testdatabase");
        Point.Builder builder = Point.measurement("student");
        Map<String, String> tags = new HashMap<String, String>();
        Map<String, Object> fields = new HashMap<String, Object>();
        tags.put("name", "xie");
        tags.put("sex", "m");

        fields.put("score", "97");
        fields.put("age", 22);
        fields.put("num", 3.1);
        builder.tag(tags);
        builder.fields(fields);
        builder.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        Point point = builder.build();
        iotDBInfluxDB.write(point);
//        io.write("1", point);
    }
}
