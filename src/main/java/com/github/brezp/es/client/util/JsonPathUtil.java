package com.github.brezp.es.client.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.jayway.jsonpath.*;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * 语法和文档见：https://github.com/json-path/JsonPath
 *
 */
public class JsonPathUtil {
    private static final Logger LOG = Logger.getLogger(JsonPathUtil.class);
    private static final Gson GSON = new Gson();

    public static ParseContext getParser() {
        Configuration configuration;
        ParseContext parser;
        configuration = Configuration.builder()
                .jsonProvider(new JacksonJsonNodeJsonProvider())
                .mappingProvider(new JacksonMappingProvider())
                .build();
        parser = JsonPath.using(configuration);
        return parser;
    }

    public static DocumentContext parseDoc(String json) {
        return getParser().parse(json);
    }

    public static String read(String json, String path) {
        JsonNode node = getParser().parse(json).read(path);

        return node instanceof TextNode ? node.textValue() : node.toString();
    }

    public static String add(String originJson, String path, String key, Object value) {
        DocumentContext doc = getParser().parse(originJson);
        String targetStr = doc.put(path, key, value).jsonString();
        LOG.debug(targetStr);
        return targetStr;
    }

    public static String update(String json, String path, Object value) {
        DocumentContext doc = getParser().parse(json);
        Object node = doc.read(path);
        LOG.debug(node);//origin  node value

        String targetStr = doc.set(path, value).jsonString();
        LOG.debug(targetStr);
        return targetStr;
    }

    public static String del(String json, String[] paths) {
        DocumentContext doc = getParser().parse(json);
        return delDocPaths(doc, paths);
    }

    private static String delDocPaths(DocumentContext doc, String[] paths) {
        if (paths.length > 0) {
            for (String path : paths) {
                try {
                    doc.delete(path);
                    LOG.debug("delete path:" + path);
                } catch (PathNotFoundException ignored) {
                }
            }
            String targetStr = doc.jsonString();
            LOG.debug(targetStr);
            return targetStr;
        }
        return doc.jsonString();
    }

    public static String del(String json, String path) {
        DocumentContext doc = getParser().parse(json);
        String targetStr = doc.delete(path).jsonString();
        LOG.debug(targetStr);
        return targetStr;
    }

    public static Map<String, String> str2map(String mapStr) {
        return GSON.fromJson(mapStr, new TypeToken<Map<String, String>>() {
        }.getType());
    }
}
