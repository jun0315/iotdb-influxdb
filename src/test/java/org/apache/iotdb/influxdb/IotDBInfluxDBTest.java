package org.apache.iotdb.influxdb;

import org.apache.iotdb.infludb.IotDBInfluxDB;
import org.junit.Before;
import org.junit.Test;

public class IotDBInfluxDBTest {
    private IotDBInfluxDB iotDBInfluxDB;

    @Before
    public void setUp() {
        //创建 连接
        iotDBInfluxDB = new IotDBInfluxDB();
    }

    @Test
    public void testInsert() {//测试数据插入
//        Map<String, String> tags = new HashMap<String, String>();
//        Map<String, Object> fields = new HashMap<String, Object>();
//        List<CodeInfo> list = new ArrayList<CodeInfo>();
//
//        CodeInfo info1 = new CodeInfo();
//        info1.setId(1L);
//        info1.setName("BANKS");
//        info1.setCode("ABC");
//        info1.setDescr("中国农业银行");
//        info1.setDescrE("ABC");
//        info1.setCreatedBy("system");
//        info1.setCreatedAt(new Date().getTime());
//
//        CodeInfo info2 = new CodeInfo();
//        info2.setId(2L);
//        info2.setName("BANKS");
//        info2.setCode("CCB");
//        info2.setDescr("中国建设银行");
//        info2.setDescrE("CCB");
//        info2.setCreatedBy("system");
//        info2.setCreatedAt(new Date().getTime());
//
//        list.add(info1);
//        list.add(info2);
//
//        for (CodeInfo info : list) {
//
//            tags.put("TAG_CODE", info.getCode());
//            tags.put("TAG_NAME", info.getName());
//
//            fields.put("ID", info.getId());
//            fields.put("NAME", info.getName());
//            fields.put("CODE", info.getCode());
//            fields.put("DESCR", info.getDescr());
//            fields.put("DESCR_E", info.getDescrE());
//            fields.put("CREATED_BY", info.getCreatedBy());
//            fields.put("CREATED_AT", info.getCreatedAt());
//
//            influxDB.insert(measurement, tags, fields);
//        }
    }

    @Test
    public void testQuery() {//测试数据查询
//        String command = "select * from sys_code";
//        QueryResult results = influxDB.query(command);
//
//        if (results.getResults() == null) {
//            return;
//        }
//        List<CodeInfo> lists = new ArrayList<CodeInfo>();
//        for (Result result : results.getResults()) {
//
//            List<Series> series = result.getSeries();
//            for (Series serie : series) {
////				Map<String, String> tags = serie.getTags();
//                List<List<Object>> values = serie.getValues();
//                List<String> columns = serie.getColumns();
//
//                lists.addAll(getQueryData(columns, values));
//            }
//        }
//
//        Assert.assertTrue((!lists.isEmpty()));
//        Assert.assertEquals(2, lists.size());
    }

}
