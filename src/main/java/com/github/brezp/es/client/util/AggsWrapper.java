package com.github.brezp.es.client.util;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.tophits.InternalTopHits;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * agg结果包装器
 *
 * @author brezp
 */
public class AggsWrapper {

    /**
     * 通用解析agg结果，目前只适用于每层agg只有一个agg的情况
     *
     * @param aggsMap es agg
     */
    public static Map<String, Object> wrapAggResult(Map<String, Aggregation> aggsMap) {
        return wrapAggResult(aggsMap, false);
    }

    public static Map<String, Object> wrapAggResult(Map<String, Aggregation> aggsMap, boolean isNeedMidDocCount) {
        Aggregation firstAggregation = aggsMap.get(aggsMap.keySet().iterator().next());
        Map<String, Object> result = new LinkedHashMap<>();
        wrapCurLevelAggregation(firstAggregation, result, isNeedMidDocCount);
        return result;
    }

    private static void wrapCurLevelAggregation(Aggregation aggregation, Map<String, Object> result, boolean isNeedMidDocCount) {
        if (aggregation instanceof SingleBucketAggregation) {
            SingleBucketAggregation singleBucketAggregation = (SingleBucketAggregation) aggregation;
            Aggregation nextAggregation = singleBucketAggregation.getAggregations().asList().get(0);
            wrapCurLevelAggregation(nextAggregation, result, isNeedMidDocCount);
        } else if (aggregation instanceof MultiBucketsAggregation) {
            MultiBucketsAggregation multiBucketsAggregation = (MultiBucketsAggregation) aggregation;
            for (MultiBucketsAggregation.Bucket bucket : multiBucketsAggregation.getBuckets()) {
                String key = bucket.getKeyAsString();
                double docCount = bucket.getDocCount();
                List<Aggregation> nextAggregations = bucket.getAggregations().asList();
                if (nextAggregations.isEmpty()) {
                    // 没有子agg的情况
                    result.put(key, docCount);
                } else {
                    Aggregation nextAggregation = nextAggregations.get(0);
                    if (isNeedMidDocCount) {
                        key = key + "\t" + docCount;
                    }
                    Map<String, Object> curResult = new LinkedHashMap<>();
                    result.put(key, curResult);
                    wrapCurLevelAggregation(nextAggregation, curResult, isNeedMidDocCount);
                }
            }
        } else if (aggregation instanceof NumericMetricsAggregation) {
            if (aggregation instanceof NumericMetricsAggregation.SingleValue) {
                NumericMetricsAggregation.SingleValue singleValue = (NumericMetricsAggregation.SingleValue) aggregation;
                String name = singleValue.getName();
                double value = singleValue.value();
                result.put(name, value);
            } else if (aggregation instanceof NumericMetricsAggregation.MultiValue) {
                if (aggregation instanceof Stats) {
                    Stats stats = (Stats) aggregation;
                    result.put("max", stats.getMax());
                    result.put("min", stats.getMin());
                    result.put("sum", stats.getSum());
                    result.put("avg", stats.getAvg());
                    result.put("count", stats.getCount());
                }
            }
        } else if (aggregation instanceof InternalTopHits) {
            InternalTopHits internalTopHits = (InternalTopHits) aggregation;
            List<Map<String, Object>> sources = new ArrayList<>();
            String key = internalTopHits.getName();
            for (SearchHit searchHit : internalTopHits.getHits().getHits()) {
                Map<String, Object> source = searchHit.getSourceAsMap();
                sources.add(source);
            }
            result.put(key, sources);
        }
    }


    /**
     * 通用解析agg结果
     *
     * @param aggsMap es agg
     */
    public static Map<String, Object> wrapMultiAggResult(Map<String, Aggregation> aggsMap) {
        return wrapMultiAggResult(aggsMap, false);
    }

    public static Map<String, Object> wrapMultiAggResult(Map<String, Aggregation> aggsMap, boolean isNeedMidDocCount) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (aggsMap.size() == 1) {
            Aggregation firstAggregation = aggsMap.get(aggsMap.keySet().iterator().next());
            wrapMultiCurLevelAggregation(firstAggregation, result, isNeedMidDocCount);
        } else {
            aggsMap.forEach((key, aggregation) -> {
                Map<String, Object> curResult = new LinkedHashMap<>();
                result.put(key, curResult);
                wrapMultiCurLevelAggregation(aggregation, curResult, isNeedMidDocCount);
            });
        }
        return result;
    }

    private static void wrapMultiCurLevelAggregation(Aggregation aggregation, Map<String, Object> result, boolean isNeedMidDocCount) {
        if (aggregation instanceof SingleBucketAggregation) {
            SingleBucketAggregation singleBucketAggregation = (SingleBucketAggregation) aggregation;
            Aggregation nextAggregation = singleBucketAggregation.getAggregations().asList().get(0);
            wrapCurLevelAggregation(nextAggregation, result, isNeedMidDocCount);
        } else if (aggregation instanceof MultiBucketsAggregation) {
            MultiBucketsAggregation multiBucketsAggregation = (MultiBucketsAggregation) aggregation;
            for (MultiBucketsAggregation.Bucket bucket : multiBucketsAggregation.getBuckets()) {
                String key = bucket.getKeyAsString();
                double docCount = bucket.getDocCount();
                List<Aggregation> nextAggregations = bucket.getAggregations().asList();
                if (nextAggregations.isEmpty()) {
                    // 没有子agg的情况
                    result.put(key, docCount);
                } else {
                    if (isNeedMidDocCount) {
                        key = key + "\t" + docCount;
                    }
                    Map<String, Object> curResult = new LinkedHashMap<>();
                    result.put(key, curResult);
                    for (Aggregation nextAggregation : nextAggregations) {
                        wrapCurLevelAggregation(nextAggregation, curResult, isNeedMidDocCount);
                    }
                }
            }
        } else if (aggregation instanceof NumericMetricsAggregation) {
            if (aggregation instanceof NumericMetricsAggregation.SingleValue) {
                NumericMetricsAggregation.SingleValue singleValue = (NumericMetricsAggregation.SingleValue) aggregation;
                String name = singleValue.getName();
                double value = singleValue.value();
                result.put(name, value);
            } else if (aggregation instanceof NumericMetricsAggregation.MultiValue) {
                if (aggregation instanceof Stats) {
                    Stats stats = (Stats) aggregation;
                    result.put("max", stats.getMax());
                    result.put("min", stats.getMin());
                    result.put("sum", stats.getSum());
                    result.put("avg", stats.getAvg());
                    result.put("count", stats.getCount());
                }
            }
        } else if (aggregation instanceof InternalTopHits) {
            InternalTopHits internalTopHits = (InternalTopHits) aggregation;
            List<Map<String, Object>> sources = new ArrayList<>();
            String key = internalTopHits.getName();
            for (SearchHit searchHit : internalTopHits.getHits().getHits()) {
                Map<String, Object> source = searchHit.getSourceAsMap();
                sources.add(source);
            }
            result.put(key, sources);
        }
    }
}
