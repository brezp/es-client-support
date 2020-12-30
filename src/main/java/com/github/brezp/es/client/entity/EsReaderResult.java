package com.github.brezp.es.client.entity;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author brezp
 */

public class EsReaderResult {
    private boolean isEnd;
    private String scrollId;
    private SearchHit[] datas;
    private long totalHit;
    /**
     * 从ES7开始，total hit的计算逻辑进行了优化。当total hit超过一定阈值的时候，lucene对中断对total hit数量
     * 的计算，而只会返回一个下限，即total hit多于多少数量的表述
     */
    private EsRelation relation = null;

    private List<EsDoc> esDocLists = new ArrayList<>();

    public EsReaderResult() {
    }

    public EsReaderResult(String scrollId, SearchHits searchHits, boolean isEnd) {
        this.scrollId = scrollId;
        this.datas = searchHits != null ? searchHits.getHits() : null;
        this.isEnd = isEnd;
        this.totalHit = searchHits != null ? searchHits.getTotalHits() : 0L;

        if (searchHits instanceof SearchHitsV79) {
            this.relation = EsRelation.parseRelation(((SearchHitsV79) searchHits).getRelation());
        }
    }

    public String getScrollId() {
        return scrollId;
    }

    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }

    public SearchHit[] getDatas() {
        return datas;
    }

    public void setDatas(SearchHit[] datas) {
        this.datas = datas;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public void setEnd(boolean isEnd) {
        this.isEnd = isEnd;
    }

    public long getTotalHit() {
        return totalHit;
    }

    public EsRelation getRelation() {
        return relation;
    }

    public int getDataSize() {
        return datas != null ? datas.length : 0;
    }

    public void setEsDocLists(List<EsDoc> esDocLists) {
        this.esDocLists = esDocLists;
    }

    public List<EsDoc> getEsDocLists() {
        return esDocLists;
    }

    @Override
    public String toString() {
        return "EsReaderResult{" +
                "isEnd=" + isEnd +
                ", scrollId='" + scrollId + '\'' +
                ", datas=" + Arrays.toString(datas) +
                '}';
    }
}
