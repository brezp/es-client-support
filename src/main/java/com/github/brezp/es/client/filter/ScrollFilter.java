package com.github.brezp.es.client.filter;


import com.github.brezp.es.client.util.JsonPathUtil;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.client.Request;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ScrollFilter extends AbstractRequestFilter {

    private static Logger LOG = Logger.getLogger(ScrollFilter.class);

    @Override
    protected boolean skipEndpoint(String endpoint) {
        return !endpoint.startsWith("/_search/scroll");
    }

    @Override
    protected String compatibleForV23(String sourceJson) {
        return sourceJson;
    }

    @Override
    protected String compatibleForV79(String sourceJson) {

        try {
            sourceJson = JsonPathUtil.del(sourceJson, new String[]{
                    "$..bool.disable_coord",
            });
            sourceJson = replaceUidById(sourceJson);
        } catch (Exception e) {
            LOG.error("sourceJson: " + sourceJson + ",method: " + "compatibleForV79" + ",exception class=" + e.getClass().getName() + ", exception message=" + e.getMessage(), e);
        }
        return sourceJson;
    }

    private String replaceUidById(String sourceJson) {

        Gson gson = new Gson();
        Map map = gson.fromJson(sourceJson, Map.class);
        List<Map> sortList = (List) map.get("sort");
        if (sortList != null) {
            for (Map sortItem : sortList) {
                if (sortItem.containsKey("_uid")) {
                    sortItem.put("_id", sortItem.get("_uid"));
                    sortItem.remove("_uid");
                }
            }
            return gson.toJson(map);
        } else {
            return sourceJson;
        }
    }


    @Override
    protected String compatibleForV17(String sourceJson) {
        String scrollId = JsonPathUtil.read(sourceJson, "$.scroll_id");
        if (Strings.isNullOrEmpty(scrollId)) {
            throw new RuntimeException("can not find scroll in request body: " + sourceJson);
        }
        return scrollId.replaceAll("\"", "");
    }

    @Override
    protected Map<String, String> compatibleForV17Parameters(Request srcRequest) throws IOException {
        String srcSource = EntityUtils.toString(srcRequest.getEntity());
        String scroll = JsonPathUtil.read(srcSource, "$.scroll");
        if (Strings.isNullOrEmpty(scroll)) {
            throw new RuntimeException("can not find scroll in request body: " + srcSource);
        }
        Map<String, String> newParameters = new HashMap<>(srcRequest.getParameters());
        newParameters.put("scroll", scroll.replaceAll("\"", ""));
        return newParameters;
    }
}
