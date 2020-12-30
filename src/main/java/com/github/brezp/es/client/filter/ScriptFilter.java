package com.github.brezp.es.client.filter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.brezp.es.client.util.JsonPathUtil;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class ScriptFilter extends AbstractRequestFilter {

    private static final Logger LOG = Logger.getLogger(ScriptFilter.class);
    private static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper();

    @Override
    protected boolean skipEndpoint(String endpoint) {
        return endpoint.startsWith("/_search/scroll");
    }

    @Override
    protected String compatibleForV23(String sourceJson) {
        return toLowVersionScriptField(sourceJson);
    }

    @Override
    protected String compatibleForV79(String sourceJson) {

        try {
            sourceJson = JsonPathUtil.del(sourceJson, new String[]{
                    "$..bool.disable_coord",
            });
        } catch (Exception e) {
            LOG.error("sourceJson: " + sourceJson + ",method: " + "compatibleForV79" + ",exception class=" + e.getClass().getName() + ", exception message=" + e.getMessage(), e);
        }
        return sourceJson;
    }

    @Override
    protected String compatibleForV17(String sourceJson) {
        return toLowVersionScriptField(sourceJson);
    }

    /**
     * replace "script_fields":{"test_script":{"script":{"source":"doc['2283_微博数'].value+doc['2283_粉丝数'].value","lang":"painless"},"ignore_failure":false}
     * with "script_fields":{"test_script":{"script":"doc['2283_微博数'].value+doc['2283_粉丝数'].value"}}
     * @param sourceJson
     * @return
     */
    private String toLowVersionScriptField(String sourceJson) {
        if (!hasScriptField(sourceJson))
            return sourceJson;

        Map<String, Object> scriptFields = new Gson().fromJson(
            JsonPathUtil.read(sourceJson, "$.script_fields"),
            new TypeToken<Map<String, Object>>(){}.getType());
        Map<String, Map<String, String>> newScriptFields = new HashMap<>();
        for (String filedName: scriptFields.keySet()) {
            newScriptFields.put(filedName, ImmutableMap.of("script",
                JsonPathUtil.read(sourceJson, String.format("$.script_fields.%s.script.source", filedName))
            ));
        }

        return JsonPathUtil.update(sourceJson, "$.script_fields", DEFAULT_OBJECT_MAPPER.convertValue(newScriptFields,
            JsonNode.class));
    }

    private boolean hasScriptField(String sourceJson) {
        return !Objects.equals(JsonPathUtil.read(sourceJson, "$[?(@.script_fields)]"), "[]");
    }
}
