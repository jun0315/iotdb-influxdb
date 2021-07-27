package org.apache.iotdb.infludb;

import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.session.Session;
import org.influxdb.dto.Point;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class IotDBInfluxDB {

    private static Session session;

    /**
     * 默认配置
     */
    public IotDBInfluxDB() {
        session = new Session("127.0.0.1", 6667, "root", "root");
    }

    /**
     * 构造函数
     *
     * @param host     域名
     * @param rpcPort  端口
     * @param username 用户名
     * @param password 密码
     */
    public IotDBInfluxDB(String host, int rpcPort, String username, String password) {
        session = new Session(host, rpcPort, username, password);
    }

    /**
     * 兼容influx协议中的write函数
     *
     * @param database 数据库名
     * @param point    influx中的point
     */
    void write(String database, Point point) {
        String measurement;
        Map<String, String> tags;
        Number time;
        Map<String, Object> fields;
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
                    time = (Number) reflectField.get(point);
                }
                System.out.println("1");
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }

        }

    }

    public static void main(String[] args) {
        Point.Builder builder = Point.measurement("11");
        Map<String, String> tags = new HashMap<String, String>();
        Map<String, Object> fields = new HashMap<String, Object>();
        tags.put("TAG_CODE", "1");
        tags.put("TAG_NAME", "2");

        fields.put("ID", "3");
        fields.put("NAME", "4");
        builder.tag(tags);
        builder.fields(fields);
        Point point = builder.build();
        var io = new IotDBInfluxDB();
        io.write("1", point);
    }
}
