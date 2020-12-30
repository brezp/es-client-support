package org.elasticsearch.client;

import com.github.brezp.es.client.entity.EsVersion;
import com.github.brezp.es.client.filter.AbstractRequestFilter;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.parser.SearchResponseParserV79;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.adjacency.AdjacencyMatrixAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.adjacency.ParsedAdjacencyMatrix;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filters.ParsedFilters;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.ParsedGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedHistogram;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.ParsedMissing;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedNested;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedReverseNested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.ParsedBinaryRange;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.date.ParsedDateRange;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.ParsedGeoDistance;
import org.elasticsearch.search.aggregations.bucket.range.ip.IpRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.InternalSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.ParsedSampler;
import org.elasticsearch.search.aggregations.bucket.significant.ParsedSignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.significant.ParsedSignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.*;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.geobounds.ParsedGeoBounds;
import org.elasticsearch.search.aggregations.metrics.geocentroid.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.geocentroid.ParsedGeoCentroid;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.ParsedMin;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.ParsedHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.ParsedHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.ParsedTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.ParsedTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.scripted.ParsedScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.ParsedStats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ParsedExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.ParsedTopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ParsedValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.ParsedSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.InternalBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.ParsedBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.ParsedPercentilesBucket;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.PercentilesBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.ParsedStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.StatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ExtendedStatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ParsedExtendedStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.derivative.DerivativePipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.derivative.ParsedDerivative;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.brezp.es.client.entity.EsVersion.V7_9;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.common.xcontent.XContentParserUtils.*;


/**
 * 基于ES 5.6版本的 RestHighLevelClient 衍化， 完善对ES 1.7、2.3、5.x 多版本的支持
 * <p>
 * github地址：
 * https://raw.githubusercontent.com/elastic/elasticsearch/v5.6.10/client/rest-high-level/src/main/java/org/elasticsearch/client/RestHighLevelClient.java
 *
 *
 */
public class RestClientProxy {

    private final RestClient client;
    private final NamedXContentRegistry registry;

    private final List<AbstractRequestFilter> filters = new LinkedList<>();
    private EsVersion esVersion = EsVersion.DEFAULT;


    /**
     * Creates a {@link RestClientProxy} given the low level {@link RestClient} that it should use to perform requests.
     */
    public RestClientProxy(RestClient restClient, EsVersion esVersion) {
        this(restClient, Collections.emptyList(), esVersion);
    }

    /**
     * Creates a {@link RestClientProxy} given the low level {@link RestClient} that it should use to perform requests and
     * a list of entries that allow to parse custom response sections added to Elasticsearch through plugins.
     */
    protected RestClientProxy(RestClient restClient, List<NamedXContentRegistry.Entry> namedXContentEntries, EsVersion esVersion) {
        this.client = Objects.requireNonNull(restClient);
        this.registry = new NamedXContentRegistry(
                Stream.of(getDefaultNamedXContents().stream(), getProvidedNamedXContents().stream(), namedXContentEntries.stream())
                        .flatMap(Function.identity()).collect(toList()));
        if (esVersion != null)
            this.esVersion = esVersion;
    }

    /**
     * Executes a bulk request using the Bulk API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html">Bulk API on elastic.co</a>
     */
    public BulkResponse bulk(BulkRequest bulkRequest, Header... headers) throws IOException {
        return performRequestAndParseEntity(bulkRequest, Request::bulk, DsBulkResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously executes a bulk request using the Bulk API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html">Bulk API on elastic.co</a>
     */
    public void bulkAsync(BulkRequest bulkRequest, ActionListener<BulkResponse> listener, Header... headers) {
        performRequestAsyncAndParseEntity(bulkRequest, Request::bulk, DsBulkResponse::fromXContent, listener, emptySet(), headers);
    }

    /**
     * Pings the remote Elasticsearch cluster and returns true if the ping succeeded, false otherwise
     */
    public boolean ping(Header... headers) throws IOException {
        return performRequest(new MainRequest(), (request) -> Request.ping(), RestClientProxy::convertExistsResponse,
                emptySet(), headers);
    }

    /**
     * Get the cluster info otherwise provided when sending an HTTP request to port 9200
     */
    public MainResponse info(Header... headers) throws IOException {
        return performRequestAndParseEntity(new MainRequest(), (request) -> Request.info(), MainResponse::fromXContent, emptySet(),
                headers);
    }

    /**
     * Retrieves a document by id using the Get API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html">Get API on elastic.co</a>
     */
    public GetResponse get(GetRequest getRequest, Header... headers) throws IOException {
        return performRequestAndParseEntity(getRequest, Request::get, GetResponse::fromXContent, singleton(404), headers);
    }

    /**
     * Asynchronously retrieves a document by id using the Get API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html">Get API on elastic.co</a>
     */
    public void getAsync(GetRequest getRequest, ActionListener<GetResponse> listener, Header... headers) {
        performRequestAsyncAndParseEntity(getRequest, Request::get, GetResponse::fromXContent, listener, singleton(404), headers);
    }

    /**
     * Checks for the existence of a document. Returns true if it exists, false otherwise
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html">Get API on elastic.co</a>
     */
    public boolean exists(GetRequest getRequest, Header... headers) throws IOException {
        return performRequest(getRequest, Request::exists, RestClientProxy::convertExistsResponse, emptySet(), headers);
    }

    /**
     * Asynchronously checks for the existence of a document. Returns true if it exists, false otherwise
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html">Get API on elastic.co</a>
     */
    public void existsAsync(GetRequest getRequest, ActionListener<Boolean> listener, Header... headers) {
        performRequestAsync(getRequest, Request::exists, RestClientProxy::convertExistsResponse, listener, emptySet(), headers);
    }

    /**
     * Index a document using the Index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html">Index API on elastic.co</a>
     */
    public IndexResponse index(IndexRequest indexRequest, Header... headers) throws IOException {
        return performRequestAndParseEntity(indexRequest, Request::index, IndexResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously index a document using the Index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html">Index API on elastic.co</a>
     */
    public void indexAsync(IndexRequest indexRequest, ActionListener<IndexResponse> listener, Header... headers) {
        performRequestAsyncAndParseEntity(indexRequest, Request::index, IndexResponse::fromXContent, listener, emptySet(), headers);
    }

    /**
     * Updates a document using the Update API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html">Update API on elastic.co</a>
     */
    public UpdateResponse update(UpdateRequest updateRequest, Header... headers) throws IOException {
        return performRequestAndParseEntity(updateRequest, Request::update, UpdateResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously updates a document using the Update API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html">Update API on elastic.co</a>
     */
    public void updateAsync(UpdateRequest updateRequest, ActionListener<UpdateResponse> listener, Header... headers) {
        performRequestAsyncAndParseEntity(updateRequest, Request::update, UpdateResponse::fromXContent, listener, emptySet(), headers);
    }

    /**
     * Deletes a document by id using the Delete api
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete.html">Delete API on elastic.co</a>
     */
    public DeleteResponse delete(DeleteRequest deleteRequest, Header... headers) throws IOException {
        return performRequestAndParseEntity(deleteRequest, Request::delete, DeleteResponse::fromXContent, Collections.singleton(404),
                headers);
    }

    /**
     * Asynchronously deletes a document by id using the Delete api
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete.html">Delete API on elastic.co</a>
     */
    public void deleteAsync(DeleteRequest deleteRequest, ActionListener<DeleteResponse> listener, Header... headers) {
        performRequestAsyncAndParseEntity(deleteRequest, Request::delete, DeleteResponse::fromXContent, listener,
                Collections.singleton(404), headers);
    }

    /**
     * Executes a search using the Search api
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html">Search API on elastic.co</a>
     */
    public SearchResponse search(SearchRequest searchRequest, Header... headers) throws IOException {
        return performRequestAndParseEntity(
            searchRequest,
            Request::search,
            esVersion == V7_9 ? SearchResponseParserV79::fromXContent : SearchResponse::fromXContent,
            emptySet(),
            headers
        );
    }

    /** 获取查询的真实语句 */
    public Request getSearchQueryRequest(SearchRequest searchRequest) throws IOException {
        return getSearchQueryRequest(searchRequest, Request::search);
    }

    /**
     * Asynchronously executes a search using the Search api
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html">Search API on elastic.co</a>
     */
    public void searchAsync(SearchRequest searchRequest, ActionListener<SearchResponse> listener, Header... headers) {
        performRequestAsyncAndParseEntity(
            searchRequest,
            Request::search,
            esVersion == V7_9 ? SearchResponseParserV79::fromXContent : SearchResponse::fromXContent,
            listener,
            emptySet(),
            headers
        );
    }

    /**
     * Executes a search using the Search Scroll api
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html">Search Scroll
     * API on elastic.co</a>
     */
    public SearchResponse searchScroll(SearchScrollRequest searchScrollRequest, Header... headers) throws IOException {
        return performRequestAndParseEntity(
            searchScrollRequest,
            Request::searchScroll,
            esVersion == V7_9 ? SearchResponseParserV79::fromXContent : SearchResponse::fromXContent,
            emptySet(),
            headers
        );
    }

    /**
     * Asynchronously executes a search using the Search Scroll api
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html">Search Scroll
     * API on elastic.co</a>
     */
    public void searchScrollAsync(SearchScrollRequest searchScrollRequest, ActionListener<SearchResponse> listener, Header... headers) {
        performRequestAsyncAndParseEntity(
            searchScrollRequest,
            Request::searchScroll,
            esVersion == V7_9 ? SearchResponseParserV79::fromXContent : SearchResponse::fromXContent,
            listener,
            emptySet(),
            headers
        );
    }

    /**
     * Clears one or more scroll ids using the Clear Scroll api
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html#_clear_scroll_api">
     * Clear Scroll API on elastic.co</a>
     */
    public ClearScrollResponse clearScroll(ClearScrollRequest clearScrollRequest, Header... headers) throws IOException {
        return performRequestAndParseEntity(clearScrollRequest, Request::clearScroll, ClearScrollResponse::fromXContent,
                emptySet(), headers);
    }

    /**
     * Asynchronously clears one or more scroll ids using the Clear Scroll api
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html#_clear_scroll_api">
     * Clear Scroll API on elastic.co</a>
     */
    public void clearScrollAsync(ClearScrollRequest clearScrollRequest, ActionListener<ClearScrollResponse> listener, Header... headers) {
        performRequestAsyncAndParseEntity(clearScrollRequest, Request::clearScroll, ClearScrollResponse::fromXContent,
                listener, emptySet(), headers);
    }

    protected <Req extends ActionRequest, Resp> Resp performRequestAndParseEntity(Req request,
                                                                                  CheckedFunction<Req, Request, IOException> requestConverter,
                                                                                  CheckedFunction<XContentParser, Resp, IOException> entityParser,
                                                                                  Set<Integer> ignores, Header... headers) throws IOException {
        return performRequest(request, requestConverter, (response) -> parseEntity(response.getEntity(), entityParser), ignores, headers);
    }

    /**
     * 获取查询的真实语句
     *
     * fixme 逻辑与下面链接方法是相同的，只是把请求省略掉了
     * {@link RestClientProxy#performRequest(ActionRequest, CheckedFunction, CheckedFunction, Set, Header...)}
     */
    protected <Req extends ActionRequest> Request getSearchQueryRequest(Req request,
                                                                        CheckedFunction<Req, Request, IOException> requestConverter) throws IOException {
        return addFilters(request, requestConverter.apply(request));

    }

    /** 去掉一些多余的查询属性 */
    protected <Req extends ActionRequest> Request addFilters(Req request, Request apply) throws IOException {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            throw validationException;
        }

        Request req = apply;
        for (AbstractRequestFilter filter : filters) {
            req = filter.filter(req, this.esVersion);
        }
        return req;
    }

    protected <Req extends ActionRequest, Resp> Resp performRequest(Req request,
                                                                    CheckedFunction<Req, Request, IOException> requestConverter,
                                                                    CheckedFunction<Response, Resp, IOException> responseConverter,
                                                                    Set<Integer> ignores, Header... headers) throws IOException {
        Request req = addFilters(request, requestConverter.apply(request));

        Response response;
        try {
            response = client.performRequest(req.getMethod(), req.getEndpoint(), req.getParameters(), req.getEntity(), headers);

        } catch (ResponseException e) {
            if (ignores.contains(e.getResponse().getStatusLine().getStatusCode())) {
                try {
                    return responseConverter.apply(e.getResponse());
                } catch (Exception innerException) {
                    //the exception is ignored as we now try to parse the response as an error.
                    //this covers cases like get where 404 can either be a valid document not found response,
                    //or an error for which parsing is completely different. We try to consider the 404 response as a valid one
                    //first. If parsing of the response breaks, we fall back to parsing it as an error.
                    throw parseResponseException(e);
                }
            }
            throw parseResponseException(e);
        }

        try {
            return responseConverter.apply(response);
        } catch (Exception e) {
            throw new IOException("Unable to parse response body for " + response, e);
        }
    }

    protected <Req extends ActionRequest, Resp> void performRequestAsyncAndParseEntity(Req request,
                                                                                       CheckedFunction<Req, Request, IOException> requestConverter,
                                                                                       CheckedFunction<XContentParser, Resp, IOException> entityParser,
                                                                                       ActionListener<Resp> listener, Set<Integer> ignores, Header... headers) {
        performRequestAsync(request, requestConverter, (response) -> parseEntity(response.getEntity(), entityParser),
                listener, ignores, headers);
    }

    protected <Req extends ActionRequest, Resp> void performRequestAsync(Req request,
                                                                         CheckedFunction<Req, Request, IOException> requestConverter,
                                                                         CheckedFunction<Response, Resp, IOException> responseConverter,
                                                                         ActionListener<Resp> listener, Set<Integer> ignores, Header... headers) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            // 对于ES7，type字段为空的校验场景，忽略掉即可
            if (!(esVersion == V7_9 &&
                validationException.validationErrors()
                    .stream()
                    .allMatch(s -> s.contains("type is missing")))) {
                listener.onFailure(validationException);
                return;
            }
        }

        Request req;
        try {
            req = requestConverter.apply(request);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        ResponseListener responseListener = wrapResponseListener(responseConverter, listener, ignores);
        client.performRequestAsync(req.getMethod(), req.getEndpoint(), req.getParameters(), req.getEntity(), responseListener, headers);
    }

    <Resp> ResponseListener wrapResponseListener(CheckedFunction<Response, Resp, IOException> responseConverter,
                                                 ActionListener<Resp> actionListener, Set<Integer> ignores) {
        return new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    actionListener.onResponse(responseConverter.apply(response));
                } catch (Exception e) {
                    IOException ioe = new IOException("Unable to parse response body for " + response, e);
                    onFailure(ioe);
                }
            }

            @Override
            public void onFailure(Exception exception) {
                if (exception instanceof ResponseException) {
                    ResponseException responseException = (ResponseException) exception;
                    Response response = responseException.getResponse();
                    if (ignores.contains(response.getStatusLine().getStatusCode())) {
                        try {
                            actionListener.onResponse(responseConverter.apply(response));
                        } catch (Exception innerException) {
                            //the exception is ignored as we now try to parse the response as an error.
                            //this covers cases like get where 404 can either be a valid document not found response,
                            //or an error for which parsing is completely different. We try to consider the 404 response as a valid one
                            //first. If parsing of the response breaks, we fall back to parsing it as an error.
                            actionListener.onFailure(parseResponseException(responseException));
                        }
                    } else {
                        actionListener.onFailure(parseResponseException(responseException));
                    }
                } else {
                    actionListener.onFailure(exception);
                }
            }
        };
    }

    /**
     * Converts a {@link ResponseException} obtained from the low level REST client into an {@link ElasticsearchException}.
     * If a response body was returned, tries to parse it as an error returned from Elasticsearch.
     * If no response body was returned or anything goes wrong while parsing the error, returns a new {@link ElasticsearchStatusException}
     * that wraps the original {@link ResponseException}. The potential exception obtained while parsing is added to the returned
     * exception as a suppressed exception. This method is guaranteed to not throw any exception eventually thrown while parsing.
     */
    protected ElasticsearchStatusException parseResponseException(ResponseException responseException) {
        Response response = responseException.getResponse();
        HttpEntity entity = response.getEntity();
        ElasticsearchStatusException elasticsearchException;
        if (entity == null) {
            elasticsearchException = new ElasticsearchStatusException(
                    responseException.getMessage(), RestStatus.fromCode(response.getStatusLine().getStatusCode()), responseException);
        } else {
            try {
                elasticsearchException = parseEntity(entity, BytesRestResponse::errorFromXContent);
                elasticsearchException.addSuppressed(responseException);
            } catch (Exception e) {
                RestStatus restStatus = RestStatus.fromCode(response.getStatusLine().getStatusCode());
                elasticsearchException = new ElasticsearchStatusException("Unable to parse response body", restStatus, responseException);
                elasticsearchException.addSuppressed(e);
            }
        }
        return elasticsearchException;
    }

    protected <Resp> Resp parseEntity(final HttpEntity entity,
                                      final CheckedFunction<XContentParser, Resp, IOException> entityParser) throws IOException {
        if (entity == null) {
            throw new IllegalStateException("Response body expected but not returned");
        }
        if (entity.getContentType() == null) {
            throw new IllegalStateException("Elasticsearch didn't return the [Content-Type] header, unable to parse response body");
        }
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(entity.getContentType().getValue());
        if (xContentType == null) {
            throw new IllegalStateException("Unsupported Content-Type: " + entity.getContentType().getValue());
        }
        try (XContentParser parser = xContentType.xContent().createParser(registry, entity.getContent())) {
            return entityParser.apply(parser);
        }
    }

    static boolean convertExistsResponse(Response response) {
        return response.getStatusLine().getStatusCode() == 200;
    }

    static List<NamedXContentRegistry.Entry> getDefaultNamedXContents() {
        Map<String, ContextParser<Object, ? extends Aggregation>> map = new HashMap<>();
        map.put(CardinalityAggregationBuilder.NAME, (p, c) -> ParsedCardinality.fromXContent(p, (String) c));
        map.put(InternalHDRPercentiles.NAME, (p, c) -> ParsedHDRPercentiles.fromXContent(p, (String) c));
        map.put(InternalHDRPercentileRanks.NAME, (p, c) -> ParsedHDRPercentileRanks.fromXContent(p, (String) c));
        map.put(InternalTDigestPercentiles.NAME, (p, c) -> ParsedTDigestPercentiles.fromXContent(p, (String) c));
        map.put(InternalTDigestPercentileRanks.NAME, (p, c) -> ParsedTDigestPercentileRanks.fromXContent(p, (String) c));
        map.put(PercentilesBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedPercentilesBucket.fromXContent(p, (String) c));
        map.put(MinAggregationBuilder.NAME, (p, c) -> ParsedMin.fromXContent(p, (String) c));
        map.put(MaxAggregationBuilder.NAME, (p, c) -> ParsedMax.fromXContent(p, (String) c));
        map.put(SumAggregationBuilder.NAME, (p, c) -> ParsedSum.fromXContent(p, (String) c));
        map.put(AvgAggregationBuilder.NAME, (p, c) -> ParsedAvg.fromXContent(p, (String) c));
        map.put(ValueCountAggregationBuilder.NAME, (p, c) -> ParsedValueCount.fromXContent(p, (String) c));
        map.put(InternalSimpleValue.NAME, (p, c) -> ParsedSimpleValue.fromXContent(p, (String) c));
        map.put(DerivativePipelineAggregationBuilder.NAME, (p, c) -> ParsedDerivative.fromXContent(p, (String) c));
        map.put(InternalBucketMetricValue.NAME, (p, c) -> ParsedBucketMetricValue.fromXContent(p, (String) c));
        map.put(StatsAggregationBuilder.NAME, (p, c) -> ParsedStats.fromXContent(p, (String) c));
        map.put(StatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedStatsBucket.fromXContent(p, (String) c));
        map.put(ExtendedStatsAggregationBuilder.NAME, (p, c) -> ParsedExtendedStats.fromXContent(p, (String) c));
        map.put(ExtendedStatsBucketPipelineAggregationBuilder.NAME,
                (p, c) -> ParsedExtendedStatsBucket.fromXContent(p, (String) c));
        map.put(GeoBoundsAggregationBuilder.NAME, (p, c) -> ParsedGeoBounds.fromXContent(p, (String) c));
        map.put(GeoCentroidAggregationBuilder.NAME, (p, c) -> ParsedGeoCentroid.fromXContent(p, (String) c));
        map.put(HistogramAggregationBuilder.NAME, (p, c) -> ParsedHistogram.fromXContent(p, (String) c));
        map.put(DateHistogramAggregationBuilder.NAME, (p, c) -> ParsedDateHistogram.fromXContent(p, (String) c));
        map.put(StringTerms.NAME, (p, c) -> ParsedStringTerms.fromXContent(p, (String) c));
        map.put(LongTerms.NAME, (p, c) -> ParsedLongTerms.fromXContent(p, (String) c));
        map.put(DoubleTerms.NAME, (p, c) -> ParsedDoubleTerms.fromXContent(p, (String) c));
        map.put(MissingAggregationBuilder.NAME, (p, c) -> ParsedMissing.fromXContent(p, (String) c));
        map.put(NestedAggregationBuilder.NAME, (p, c) -> ParsedNested.fromXContent(p, (String) c));
        map.put(ReverseNestedAggregationBuilder.NAME, (p, c) -> ParsedReverseNested.fromXContent(p, (String) c));
        map.put(GlobalAggregationBuilder.NAME, (p, c) -> ParsedGlobal.fromXContent(p, (String) c));
        map.put(FilterAggregationBuilder.NAME, (p, c) -> ParsedFilter.fromXContent(p, (String) c));
        map.put(InternalSampler.PARSER_NAME, (p, c) -> ParsedSampler.fromXContent(p, (String) c));
        map.put(GeoGridAggregationBuilder.NAME, (p, c) -> ParsedGeoHashGrid.fromXContent(p, (String) c));
        map.put(RangeAggregationBuilder.NAME, (p, c) -> ParsedRange.fromXContent(p, (String) c));
        map.put(DateRangeAggregationBuilder.NAME, (p, c) -> ParsedDateRange.fromXContent(p, (String) c));
        map.put(GeoDistanceAggregationBuilder.NAME, (p, c) -> ParsedGeoDistance.fromXContent(p, (String) c));
        map.put(FiltersAggregationBuilder.NAME, (p, c) -> ParsedFilters.fromXContent(p, (String) c));
        map.put(AdjacencyMatrixAggregationBuilder.NAME, (p, c) -> ParsedAdjacencyMatrix.fromXContent(p, (String) c));
        map.put(SignificantLongTerms.NAME, (p, c) -> ParsedSignificantLongTerms.fromXContent(p, (String) c));
        map.put(SignificantStringTerms.NAME, (p, c) -> ParsedSignificantStringTerms.fromXContent(p, (String) c));
        map.put(ScriptedMetricAggregationBuilder.NAME, (p, c) -> ParsedScriptedMetric.fromXContent(p, (String) c));
        map.put(IpRangeAggregationBuilder.NAME, (p, c) -> ParsedBinaryRange.fromXContent(p, (String) c));
        map.put(TopHitsAggregationBuilder.NAME, (p, c) -> ParsedTopHits.fromXContent(p, (String) c));
        List<NamedXContentRegistry.Entry> entries = map.entrySet().stream()
                .map(entry -> new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(entry.getKey()), entry.getValue()))
                .collect(Collectors.toList());
        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(TermSuggestion.NAME),
                (parser, context) -> TermSuggestion.fromXContent(parser, (String) context)));
        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(PhraseSuggestion.NAME),
                (parser, context) -> PhraseSuggestion.fromXContent(parser, (String) context)));
        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(CompletionSuggestion.NAME),
                (parser, context) -> CompletionSuggestion.fromXContent(parser, (String) context)));
        return entries;
    }

    /**
     * Loads and returns the {@link NamedXContentRegistry.Entry} parsers provided by plugins.
     */
    static List<NamedXContentRegistry.Entry> getProvidedNamedXContents() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        for (NamedXContentProvider service : ServiceLoader.load(NamedXContentProvider.class)) {
            entries.addAll(service.getNamedXContentParsers());
        }
        return entries;
    }

    public List<AbstractRequestFilter> getFilters() {
        return filters;
    }

    public void addFilters(List<AbstractRequestFilter> otherFilters) {
        if (otherFilters != null && !otherFilters.isEmpty())
            this.filters.addAll(otherFilters);
    }

    public void addFilter(AbstractRequestFilter filter) {
        if (filter != null)
            this.filters.add(filter);
    }

    public void useVersion(EsVersion esVersion) {
        this.esVersion = esVersion;
    }

    public EsVersion getEsVersion() {
        return esVersion;
    }

    /**
     * 兼容v1.7的的错误信息，当不需要支持v1.7的时候可以去掉
     */
    static class DsBulkResponse {

        private static final String ITEMS = "items";
        private static final String ERRORS = "errors";
        private static final String TOOK = "took";
        private static final String INGEST_TOOK = "ingest_took";

        static final long NO_INGEST_TOOK = -1L;

        static BulkResponse fromXContent(XContentParser parser) throws IOException {
            Token token = parser.nextToken();
            ensureExpectedToken(Token.START_OBJECT, token, parser::getTokenLocation);

            long took = -1L;
            long ingestTook = NO_INGEST_TOOK;
            List<BulkItemResponse> items = new ArrayList<>();

            String currentFieldName = parser.currentName();
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (TOOK.equals(currentFieldName)) {
                        took = parser.longValue();
                    } else if (INGEST_TOOK.equals(currentFieldName)) {
                        ingestTook = parser.longValue();
                    } else if (!ERRORS.equals(currentFieldName)) {
                        throwUnknownField(currentFieldName, parser.getTokenLocation());
                    }
                } else if (token == Token.START_ARRAY) {
                    if (ITEMS.equals(currentFieldName)) {
                        while (parser.nextToken() != Token.END_ARRAY) {
                            items.add(DsBulkItemResponse.fromXContent(parser, items.size()));
                        }
                    } else {
                        throwUnknownField(currentFieldName, parser.getTokenLocation());
                    }
                } else {
                    throwUnknownToken(token, parser.getTokenLocation());
                }
            }
            return new BulkResponse(items.toArray(new BulkItemResponse[0]), took, ingestTook);
        }
    }

    /**
     * 兼容v1.7的的错误信息，当不需要支持v1.7的时候可以去掉
     */
    static class DsBulkItemResponse {

        private static final String STATUS = "status";
        private static final String ERROR = "error";

        /**
         *
         * @param parser the {@link XContentParser}
         * @param id the id to assign to the parsed {@link DsBulkResponse}. It is usually the index
         * of the item in the {@link BulkResponse#getItems} array.
         */
        static BulkItemResponse fromXContent(XContentParser parser, int id) throws IOException {
            ensureExpectedToken(Token.START_OBJECT, parser.currentToken(),
                parser::getTokenLocation);

            Token token = parser.nextToken();
            ensureExpectedToken(Token.FIELD_NAME, token, parser::getTokenLocation);

            String currentFieldName = parser.currentName();
            token = parser.nextToken();

            final OpType opType = OpType.fromString(currentFieldName);
            ensureExpectedToken(Token.START_OBJECT, token, parser::getTokenLocation);

            DocWriteResponse.Builder builder = null;
            CheckedConsumer<XContentParser, IOException> itemParser = null;

            if (opType == OpType.INDEX || opType == OpType.CREATE) {
                final IndexResponse.Builder indexResponseBuilder = new IndexResponse.Builder();
                builder = indexResponseBuilder;
                itemParser = (indexParser) -> IndexResponse.parseXContentFields(indexParser, indexResponseBuilder);
            } else if (opType == OpType.UPDATE) {
                final UpdateResponse.Builder updateResponseBuilder = new UpdateResponse.Builder();
                builder = updateResponseBuilder;
                itemParser = (updateParser) -> UpdateResponse.parseXContentFields(updateParser, updateResponseBuilder);
            } else if (opType == OpType.DELETE) {
                final DeleteResponse.Builder deleteResponseBuilder = new DeleteResponse.Builder();
                builder = deleteResponseBuilder;
                itemParser = (deleteParser) -> DeleteResponse.parseXContentFields(deleteParser, deleteResponseBuilder);
            } else {
                throwUnknownField(currentFieldName, parser.getTokenLocation());
            }

            RestStatus status = null;
            ElasticsearchException exception = null;
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                }

                if (ERROR.equals(currentFieldName)) {
                    if (token == Token.START_OBJECT) {
                        exception = ElasticsearchException.fromXContent(parser);
                    } else if (token == Token.VALUE_STRING) {
                        exception = new ElasticsearchException(parser.text());
                    }
                } else if (STATUS.equals(currentFieldName)) {
                    if (token == Token.VALUE_NUMBER) {
                        status = RestStatus.fromCode(parser.intValue());
                    }
                } else {
                    itemParser.accept(parser);
                }
            }

            ensureExpectedToken(Token.END_OBJECT, token, parser::getTokenLocation);
            token = parser.nextToken();
            ensureExpectedToken(Token.END_OBJECT, token, parser::getTokenLocation);

            BulkItemResponse bulkItemResponse;
            if (exception != null) {
                Failure failure = new Failure(builder.getShardId().getIndexName(), builder.getType(), builder.getId(), exception);
                setStatus(failure, status);
                bulkItemResponse = new BulkItemResponse(id, opType, failure);
            } else {
                bulkItemResponse = new BulkItemResponse(id, opType, builder.build());
            }
            return bulkItemResponse;
        }

        private static void setStatus(Failure failure, RestStatus status) {
            try {
                Field field = failure.getClass().getDeclaredField("status");
                field.setAccessible(true);
                field.set(failure, status);
                field.setAccessible(false);
            } catch (NoSuchFieldException | IllegalAccessException ignored) {
            }
        }
    }
}