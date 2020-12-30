package com.github.brezp.es.client.filter;

import com.github.brezp.es.client.util.JsonPathUtil;
import com.google.gson.Gson;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 *
 * 可以参考 JsonPathUtilTest
 *
 * @author sugan
 * @since 2018-07-06.
 */
public class AggFilter extends AbstractRequestFilter {

    private static Logger LOG = Logger.getLogger(AggFilter.class);

    @Override
    protected String compatibleForV23(String sourceJson) {
        String[] deletePaths = new String[]{
                "$.aggregations..date_histogram.offset",
                "$.query..ignore_unmapped",
                "$..exists.boost",
        };
        sourceJson = JsonPathUtil.del(sourceJson, deletePaths);
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
        //1. 如果json包含agg, 则查找在v1.7有异常的字段，进行修正
        String[] deletePaths = new String[]{
                "$.aggregations..date_histogram.offset",
                "$..bool..boost",
                "$..bool..disable_coord",
                "$..bool..adjust_pure_negative",
                "$..ignore_unmapped",
                "$..has_parent.score",
        };

        sourceJson = JsonPathUtil.del(sourceJson, deletePaths);
        sourceJson = replaceFunctionScoreQuery2Filter(sourceJson);
        return sourceJson;
    }

    private String replaceFunctionScoreQuery2Filter(String sourceJson) {
        try {
            if(sourceJson.contains("query") && sourceJson.contains("function_score")) {
                sourceJson = sourceJson.replaceAll("query", "filter");
                sourceJson = sourceJson.replaceFirst("filter", "query");
            }
        } catch (Exception e) {
            LOG.info("sourceJson:\t" + sourceJson + ",rule:" + "replace function_score's query to filter.");
            e.printStackTrace();
        }
        return sourceJson;
    }
}
