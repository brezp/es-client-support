package com.github.brezp.es.client.api;


import com.github.brezp.es.client.base.DocScanner;
import com.github.brezp.es.client.base.EsClient;
import com.github.brezp.es.client.entity.EsDoc;
import com.github.brezp.es.client.entity.EsReaderResult;
import com.github.brezp.es.client.entity.EsSort;
import com.github.brezp.es.client.entity.EsSuggest;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;

import java.io.IOException;
import java.util.*;

import static com.github.brezp.es.client.entity.EsVersion.V7_9;

public class ReaderApi {

    private static final Logger LOG = Logger.getLogger(ReaderApi.class);

    private final SearchModule searchModule = new SearchModule(Settings.EMPTY, false,
        Collections.emptyList());

    private final EsClient client;

    private static final int DEFAULT_SCROLL_SIZE = 1000;
    private static final long DEFAULT_TIMEOUT_MILLS = 120 * 1000;

    private final long timeoutMills;

    private final String[] indices;
    private final String indexType;

    /**
     * indices options，如果没有设置，则使用默认的
     */
    private Map<String, Object> indicesOptions = new HashMap<>();

    public ReaderApi(EsClient esClient, String indexName, String indexType) {
        this(esClient, new String[]{indexName}, indexType, DEFAULT_TIMEOUT_MILLS);
    }

    public ReaderApi(EsClient esClient, String[] indices, String indexType) {
        this(esClient, indices, indexType, DEFAULT_TIMEOUT_MILLS);
    }

    public ReaderApi(EsClient esClient, String indexName, String indexType, long timeoutMills) {
        this(esClient, new String[]{indexName}, indexType, timeoutMills);
    }

    public ReaderApi(EsClient esClient, String[] indices, String indexType, long timeoutMills) {
        this.client = esClient;
        this.indices = indices;
        this.timeoutMills = timeoutMills;
        this.indexType = indexType;
    }

    public void close() {
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public ReaderApi setIndicesOption(String name, Object value) {
        this.indicesOptions.put(name, value);
        return this;
    }

    //---------------------------------获取满足查询条件的数量-----------------------------------
    public long count(QueryBuilder queryBuilder) throws Exception {
        EsReaderResult rst = search(null, 0, queryBuilder, null, null, null);
        return rst.getTotalHit();
    }
    //---------------------------------------------------------------------------------------


    //---------------------------获取满足查询条件的数据（search模式）-----------------------------
    public EsReaderResult search(Integer from,
        Integer size,
        QueryBuilder builder,
        String[] includeSource,
        EsSort esSort,
        Map<String, String> scriptFields) throws Exception {
        return search(from, size, builder, includeSource, esSort, scriptFields, null);
    }

    public EsReaderResult search(Integer from,
        Integer size,
        QueryBuilder builder,
        String[] includeSource,
        EsSort esSort,
        Map<String, String> scriptFields, HighlightBuilder highlightBuilder) throws Exception {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(builder);

        // 搜索条件
        if (from != null) {
            sourceBuilder.from(from);
        }

        if (includeSource != null) {
            sourceBuilder.fetchSource(includeSource, new String[0]);
        }

        if (size != null) {
            sourceBuilder.size(size);
        }

        if (esSort != null) {
            fillSort(esSort, sourceBuilder);
        }

        //支持script field
        if (scriptFields != null && !scriptFields.isEmpty()) {
            for (String field : scriptFields.keySet()) {
                sourceBuilder.scriptField(field, new Script(scriptFields.get(field)));
            }
        }
        //支持高亮
        if (highlightBuilder != null) {
            sourceBuilder.highlighter(highlightBuilder);
        }
        return search(sourceBuilder);
    }

    private void fillSort(EsSort esSort, SearchSourceBuilder sourceBuilder) {
        for (Tuple<String, SortOrder> sort : esSort.sortPairs()) {
            sourceBuilder.sort(sort.v1(), sort.v2());
        }
    }

    public EsReaderResult search(String query) throws Exception {
        SearchSourceBuilder builder;
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(new NamedXContentRegistry(searchModule
                .getNamedXContents()), query)) {
            builder = SearchSourceBuilder.fromXContent(new QueryParseContext(parser));
        }

        return search(builder);
    }

    private EsReaderResult search(SearchSourceBuilder sourceBuilder) throws IOException {
        if (timeoutMills != 0) {
            sourceBuilder.timeout(new TimeValue(timeoutMills));
        }

        SearchRequest request = getSearchRequest(sourceBuilder, null);

        LOG.debug(request.source().toString());

        SearchResponse response = client.getClient().search(request);

        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        EsReaderResult esReaderResult = new EsReaderResult(null, response.getHits(), true);

        if (response.getFailedShards() != 0) {
            //只要有一个fail，就返回空list
            LOG.error("fail shards " + response.getFailedShards());
            return esReaderResult;
        }

        if (searchHits.length > 0) {
            List<EsDoc> docs = new ArrayList<>(searchHits.length);
            for (SearchHit searchHit : searchHits) {
                if (searchHit.getSource() != null) {
                    EsDoc newDoc = new EsDoc(searchHit.getId(), searchHit.getSource());
                    //有source的情况也可能有field
                    if (sourceBuilder.scriptFields() != null
                        && sourceBuilder.scriptFields().size() > 0) {
                        fetchScriptField(searchHit, newDoc);
                    }
                    docs.add(newDoc);
                } else {
                    Map<String, SearchHitField> name2field = searchHit.getFields();
                    Map<String, Object> name2value = new HashMap<>();
                    for (String name : name2field.keySet()) {
                        SearchHitField hitField = name2field.get(name);
                        name2value.put(name, hitField.getValue());
                    }
                    docs.add(new EsDoc(searchHit.getId(), name2value));
                }
            }
            esReaderResult.setEsDocLists(docs);
        }
        return esReaderResult;
    }

    private void fetchScriptField(SearchHit searchHit, EsDoc newDoc) {
        Map<String, SearchHitField> name2field = searchHit.getFields();
        if (name2field != null && !name2field.isEmpty()) {
            for (String name : name2field.keySet()) {
                SearchHitField hitField = name2field.get(name);
                newDoc.put(name, hitField.getValue());
            }
        }
    }

    public EsReaderResult search(QueryBuilder builder) throws Exception {
        return search(null, null, builder, null, null, null);
    }

    public EsReaderResult search(int from, int size, QueryBuilder builder) throws Exception {
        return search(from, size, builder, null, null, null);
    }

    //---------------------------------------------------------------------------------------


    //---------------------------获取满足查询条件的数据（scroll模式）-----------------------------
    public EsReaderResult scroll(QueryBuilder builder, int scrollSize, boolean isFetchSource,
        String[] includeSources) throws Exception {
        return scroll(builder, scrollSize, null, isFetchSource, includeSources, null, null);
    }

    public EsReaderResult scroll(QueryBuilder builder, int scrollSize, EsSort esSort,
        boolean isFetchSource, String[] includeSources, HighlightBuilder highlightBuilder,
        Map<String, String> scriptFields) throws Exception {
        SearchRequest request = getScrollRequest(builder, scrollSize, esSort, isFetchSource,
            includeSources, highlightBuilder, scriptFields);
        SearchResponse scrollResp = client.getClient().search(request);

        String scrollId = scrollResp.getScrollId();
        SearchHits searchHits = scrollResp.getHits();
        SearchHit[] searchHitData = searchHits.getHits();
        boolean isEnd = false;
        if (searchHitData.length == 0) {
            isEnd = true;
        }
        return new EsReaderResult(scrollId, searchHits, isEnd);
    }

    public SearchRequest getScrollRequest(QueryBuilder builder, int scrollSize,
        boolean isFetchSource, String[] includeSources) {
        return getScrollRequest(builder, scrollSize, isFetchSource, includeSources, null);
    }

    public SearchRequest getScrollRequest(QueryBuilder builder, int scrollSize,
        boolean isFetchSource, String[] includeSources, HighlightBuilder highlightBuilder) {
        return getScrollRequest(builder, scrollSize, null, isFetchSource, includeSources,
            highlightBuilder, null);
    }

    public SearchRequest getScrollRequest(QueryBuilder builder, int scrollSize, EsSort esSort,
        boolean isFetchSource, String[] includeSources, HighlightBuilder highlightBuilder,
        Map<String, String> scriptFields) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(builder)
            .size(scrollSize)
            .fetchSource(isFetchSource)
            .timeout(new TimeValue(timeoutMills));

        if (esSort != null) {
            fillSort(esSort, sourceBuilder);
        }

        if (isFetchSource) {
            if (includeSources != null) {
                sourceBuilder.fetchSource(includeSources, new String[0]);
            }
        }
        if (highlightBuilder != null) {
            sourceBuilder.highlighter(highlightBuilder);
        }

        //支持script field
        if (scriptFields != null && !scriptFields.isEmpty()) {
            sourceBuilder.fetchSource(true);
            for (String field : scriptFields.keySet()) {
                sourceBuilder.scriptField(field, new Script(scriptFields.get(field)));
            }
        }

        SearchRequest request = getSearchRequest(sourceBuilder, null)
            .scroll(new TimeValue(timeoutMills));
        LOG.debug(request.source().toString());

        return request;
    }

    public void scroll(QueryBuilder builder, boolean isFetchSource, String[] includeSources,
                       DocScanner scanner, long range, Map<String, String> scriptFields) throws Exception {
        scroll(builder, DEFAULT_SCROLL_SIZE, null, isFetchSource, includeSources, scanner, range,
            scriptFields);
    }

    public void scroll(QueryBuilder builder, int scrollSize, EsSort esSort, boolean isFetchSource,
        String[] includeSources, DocScanner scanner, long range, Map<String, String> scriptFields)
        throws Exception {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(builder)
            .size(scrollSize)
            .fetchSource(isFetchSource)
            .timeout(new TimeValue(timeoutMills));

        if (esSort != null) {
            fillSort(esSort, sourceBuilder);
        }

        if (isFetchSource) {
            if (includeSources != null) {
                sourceBuilder.fetchSource(includeSources, new String[0]);
            }
        }

        //支持script field
        boolean hasScriptField = false;
        if (scriptFields != null && !scriptFields.isEmpty()) {
            sourceBuilder.fetchSource(true);
            for (String field : scriptFields.keySet()) {
                sourceBuilder.scriptField(field, new Script(scriptFields.get(field)));
            }
            hasScriptField = true;
        }

        SearchRequest request = getSearchRequest(sourceBuilder, null)
            .scroll(new TimeValue(timeoutMills)).searchType(SearchType.DEFAULT);
        LOG.debug(request.source().toString());
        SearchResponse scrollResp = client.getClient().search(request);

        int i = 0;
        while (true) {
            if (scanner.stop) {
                return;
            }
            try {
                scrollResp = client.getClient().searchScroll(
                    new SearchScrollRequest(scrollResp.getScrollId())
                        .scroll(new TimeValue(timeoutMills)));
                SearchHit[] hits = scrollResp.getHits().getHits();
                if (hits.length == 0) {
                    LOG.info("0 hits, exit");
                    return;
                } else {
                    for (SearchHit hit : hits) {
                        try {
                            EsDoc esDoc = new EsDoc(hit.getId(), hit.getSource());

                            //有source的情况也可能有field
                            if (hasScriptField) {
                                fetchScriptField(hit, esDoc);
                            }

                            scanner.scanDoc(esDoc);
                        } catch (Exception e) {
                            LOG.error(e.getMessage(), e);
                        }
                        if (range != -1 && ++i >= range) {
                            LOG.info("match range: " + i + ", exit");
                            return;
                        }

                    }
                }
            } catch (Exception e) {
                LOG.error(e);
            }
        }
    }

    public EsReaderResult scroll(QueryBuilder builder, int size) throws Exception {
        return scroll(builder, size, true, null);
    }

    public EsReaderResult scroll(QueryBuilder builder) throws Exception {
        return scroll(builder, DEFAULT_SCROLL_SIZE, true, null);
    }

    public EsReaderResult scroll(QueryBuilder builder, int size, String[] includes)
        throws Exception {
        return scroll(builder, size, true, includes);
    }

    public EsReaderResult scroll(String scrollId) throws Exception {
        if (scrollId == null) {
            throw new NullPointerException("scrollid can not be null");
        }
        boolean isEnd = false;
        TimeValue timeValue = new TimeValue(timeoutMills);

        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        scrollRequest.scroll(timeValue);

        LOG.debug(scrollRequest.toString());
        SearchResponse response = client.getClient().searchScroll(scrollRequest);

        SearchHits searchHits = response.getHits();
        SearchHit[] searchHitsData = searchHits.getHits();
        if (searchHitsData.length == 0) {
            isEnd = true;
        } else {
            scrollId = response.getScrollId();
        }

        return new EsReaderResult(scrollId, searchHits, isEnd);
    }

    //---------------------------------------------------------------------------------------


    //----------------------------获取查询条件的数据（scroll模式）-------------------------------
    public Map<String, Aggregation> aggSearch(QueryBuilder queryBuilder,
        AggregationBuilder aggregationBuilder, boolean isCache) throws Exception {

        SearchResponse response = getAggSearchResponse(queryBuilder, aggregationBuilder, isCache);

        if (response.getFailedShards() != 0) { //只要有一个fail，就返回空list
            LOG.error("fail shards " + response.getFailedShards());
            return null;
        }

        return response.getAggregations().getAsMap();
    }

    public Map<String, Aggregation> aggSearch(QueryBuilder queryBuilder,
        AggregationBuilder aggregationBuilder) throws Exception {
        return aggSearch(queryBuilder, aggregationBuilder, false);
    }

    public Map<String, Aggregation> aggSearch(QueryBuilder queryBuilder,
        List<AggregationBuilder> aggregationBuilderList) throws Exception {
        return aggSearch(queryBuilder, aggregationBuilderList, false);
    }

    public Map<String, Aggregation> aggSearch(QueryBuilder queryBuilder,
        List<AggregationBuilder> aggregationBuilderList, boolean isCache) throws Exception {
        SearchRequest request = getSearchRequest(queryBuilder, aggregationBuilderList, isCache);

        SearchResponse response = client.getClient().search(request);

        if (response.getFailedShards() != 0) { //只要有一个fail，就返回空list
            LOG.error("fail shards " + response.getFailedShards());
            return null;
        }

        return response.getAggregations().getAsMap();
    }

    //---------------------------------------------------------------------------------------


    public SearchResponse getAggSearchResponse(QueryBuilder queryBuilder,
        AggregationBuilder aggregationBuilder, boolean isCache) throws Exception {
        return getAggSearchResponse(queryBuilder, Collections.singletonList(aggregationBuilder),
            isCache);
    }

    public SearchResponse getAggSearchResponse(QueryBuilder queryBuilder,
        List<AggregationBuilder> aggregationBuilderList, boolean isCache) throws Exception {
        SearchRequest request = getSearchRequest(queryBuilder, aggregationBuilderList, isCache);

        return client.getClient().search(request);
    }

    public SearchRequest getSearchRequest(QueryBuilder queryBuilder,
        List<AggregationBuilder> aggregationBuilderList, boolean isCache) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(queryBuilder)
            .size(0)
            .timeout(new TimeValue(timeoutMills));

        for (AggregationBuilder aggregationBuilder : aggregationBuilderList) {
            sourceBuilder.aggregation(aggregationBuilder);
        }

        SearchRequest request = getSearchRequest(sourceBuilder, isCache);

        LOG.debug(request.source().toString());

        return request;
    }

    /**
     * 创建基础、通用的search request对象，方便添加全局统一的参数
     * @param sourceBuilder buider
     * @param isCache 是否缓存request，如果为null，则使用默认参数
     * @return search request
     */
    private SearchRequest getSearchRequest(SearchSourceBuilder sourceBuilder, Boolean isCache) {
        SearchRequest request = new SearchRequest(this.indices)
            .source(sourceBuilder);

        // 对于ES7来说，并没有type字段，但是由于commons3-es是基于ES5的RestClient开发的，会对请求进行一些合法
        // 性校验，例如必须包含type字段等
        if (client.getClient().getEsVersion() != V7_9) {
            request.types(this.indexType);
        }

        if (isCache != null) {
            request.requestCache(isCache);
        }

        if (indicesOptions != null && indicesOptions.size() > 0) {
            request.indicesOptions(IndicesOptions.fromMap(indicesOptions, request.indicesOptions()));
        }

        return request;
    }

    public Request getAggSearchQueryRequest(QueryBuilder queryBuilder,
        List<AggregationBuilder> aggregationBuilderList) throws Exception {
        SearchRequest request = getSearchRequest(queryBuilder, aggregationBuilderList, false);

        return client.getClient().getSearchQueryRequest(request);
    }

    public Request getSearchQueryRequest(QueryBuilder builder) throws Exception {
        return getSearchQueryRequest(builder, DEFAULT_SCROLL_SIZE, true, null);
    }

    public Request getSearchQueryRequest(QueryBuilder builder, HighlightBuilder highlightBuilder)
        throws Exception {
        return getSearchQueryRequest(builder, DEFAULT_SCROLL_SIZE, true, null, highlightBuilder);
    }

    public Request getSearchQueryRequest(QueryBuilder builder, int scrollSize,
        boolean isFetchSource, String[] includeSources, HighlightBuilder highlightBuilder)
        throws Exception {
        SearchRequest request = getScrollRequest(builder, scrollSize, isFetchSource, includeSources,
            highlightBuilder);
        return client.getClient().getSearchQueryRequest(request);
    }

    public Request getSearchQueryRequest(QueryBuilder builder, int scrollSize,
        boolean isFetchSource, String[] includeSources) throws Exception {
        SearchRequest request = getScrollRequest(builder, scrollSize, isFetchSource,
            includeSources);

        return client.getClient().getSearchQueryRequest(request);
    }

    /**
     * test case: com.datastory.common3.es.api.ReaderApiTest#testSuggest()
     *
     * @param esSuggests 关键词列表
     * @return 搜过结果
     */
    public Suggest suggestSearch(List<EsSuggest> esSuggests) throws Exception {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        if (timeoutMills != 0) {
            sourceBuilder.timeout(new TimeValue(timeoutMills));
        }

        SuggestBuilder suggestBuilder = new SuggestBuilder();
        for (EsSuggest esSuggest : esSuggests) {
            suggestBuilder.addSuggestion(esSuggest.getAggField(), esSuggest.getSuggestion());
        }
        sourceBuilder.suggest(suggestBuilder);

        SearchRequest request = getSearchRequest(sourceBuilder, null);
        SearchResponse search = client.getClient().search(request);

        return search.getSuggest();
    }

}
