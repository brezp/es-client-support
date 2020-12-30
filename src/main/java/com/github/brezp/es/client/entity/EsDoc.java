package com.github.brezp.es.client.entity;

import org.apache.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by turner on 2018/6/28.
 * es doc Object
 */

public class EsDoc {
    private static Logger LOG = Logger.getLogger(EsDoc.class);

    private Map<String, Object> dataMap = new HashMap<>();

    public EsDoc(String id, Map<String, Object> dataMap) {
        this.dataMap = dataMap;
        this.dataMap.put("id", id);
    }

    public EsDoc(Map<String, Object> dataMap) {
        this.dataMap = dataMap;
    }

    public EsDoc(EsDoc esDoc) {
        dataMap.putAll(esDoc.dataMap);
        //深拷贝，不能把id也拷贝过来
        dataMap.remove("id");
    }

    public EsDoc() {
    }

    public EsDoc(String id) {
        dataMap.put("id", id);
    }

    public String getId() {
        return (String) dataMap.get("id");
    }

    public boolean containsKey(String key) {
        return dataMap.containsKey(key);
    }

    public Object get(String key) {
        return dataMap.get(key);
    }

    public void put(String key, Object value) {
        dataMap.put(key, value);
    }

    public Object remove(String key) {
        return dataMap.remove(key);
    }

    public int size() {
        return dataMap.size();
    }

    public String toJson() {
        try {
            XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
            jsonBuild.startObject();
            for (Map.Entry<String, Object> e : dataMap.entrySet()) {
                jsonBuild.field(e.getKey(), e.getValue());
            }
            jsonBuild.endObject();
            return jsonBuild.string();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return null;
    }

    public void setDataMap(Map<String, Object> dataMap) {
        this.dataMap = dataMap;
    }

    public Map<String, Object> getDataMap() {
        return dataMap;
    }
}
