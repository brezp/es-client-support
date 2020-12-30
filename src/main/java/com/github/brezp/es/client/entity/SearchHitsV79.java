package com.github.brezp.es.client.entity;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class SearchHitsV79 extends SearchHits  {
    /**
     * ES7 hits.total.relation字段，作为保留字段使用
     */
    private final String relation;

    public SearchHitsV79(SearchHit[] hits, long totalHits, float maxScore) {
        super(hits, totalHits, maxScore);
        this.relation = null;
    }

    public SearchHitsV79(SearchHit[] hits, Tuple<Long, String> totalHits, float maxScore) {
        super(hits, totalHits.v1(), maxScore);
        this.relation = totalHits.v2();
    }

    public String getRelation() {
        return relation;
    }
}
