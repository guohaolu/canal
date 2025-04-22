package com.alibaba.otter.canal.client.adapter.ewayt.clickhouse.support;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.util.LinkedCaseInsensitiveMap;

import com.alibaba.otter.canal.client.adapter.support.Dml;

public class SingleDml {

    private String              destination;
    private String              database;
    private String              table;
    private String              type;
    private Map<String, Object> data;
    private Map<String, Object> old;

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public Map<String, Object> getOld() {
        return old;
    }

    public void setOld(Map<String, Object> old) {
        this.old = old;
    }

    /**
     * 将一个DML对象拆分为多个SingleDml对象
     * 此方法用于处理包含多行数据的DML，将其拆分成多个单行的SingleDml对象
     * 如果dml的数据为null且类型不是"TRUNCATE"，则返回空列表
     *
     * @param dml 待拆分的DML对象
     * @param caseInsensitive 是否忽略字段大小写
     * @return 拆分后的SingleDml对象列表
     */
    public static List<SingleDml> dml2SingleDmls(Dml dml, boolean caseInsensitive) {
        List<SingleDml> singleDmls = new ArrayList<>();
        // 检查DML数据是否非空，进行拆分处理
        if (dml.getData() != null) {
            int size = dml.getData().size();
            for (int i = 0; i < size; i++) {
                SingleDml singleDml = new SingleDml();
                singleDml.setDestination(dml.getDestination());
                singleDml.setDatabase(dml.getDatabase());
                singleDml.setTable(dml.getTable());
                singleDml.setType(dml.getType());
                Map<String, Object> data = dml.getData().get(i);
                // 根据caseInsensitive参数决定是否将字段名转换为大小写不敏感
                if (caseInsensitive) {
                    data = toCaseInsensitiveMap(data);
                }
                singleDml.setData(data);
                // 处理旧数据，如果存在
                if (dml.getOld() != null) {
                    Map<String, Object> oldData = dml.getOld().get(i);
                    if (caseInsensitive) {
                        oldData = toCaseInsensitiveMap(oldData);
                    }
                    singleDml.setOld(oldData);
                }
                singleDmls.add(singleDml);
            }
        } else if ("TRUNCATE".equalsIgnoreCase(dml.getType())) {
            // 处理TRUNCATE语句，创建一个简单的SingleDml对象，不包含数据
            SingleDml singleDml = new SingleDml();
            singleDml.setDestination(dml.getDestination());
            singleDml.setDatabase(dml.getDatabase());
            singleDml.setTable(dml.getTable());
            singleDml.setType(dml.getType());
            singleDmls.add(singleDml);
        }
        return singleDmls;
    }

    public static List<SingleDml> dml2SingleDmls(Dml dml) {
        return dml2SingleDmls(dml, false);
    }

    private static <V> LinkedCaseInsensitiveMap<V> toCaseInsensitiveMap(Map<String, V> data) {
        LinkedCaseInsensitiveMap map = new LinkedCaseInsensitiveMap();
        map.putAll(data);
        return map;
    }
}
