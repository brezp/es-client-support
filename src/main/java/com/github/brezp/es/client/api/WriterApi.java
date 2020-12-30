package com.github.brezp.es.client.api;


import com.github.brezp.es.client.base.EsClient;
import com.github.brezp.es.client.entity.AddDocOpType;
import com.github.brezp.es.client.entity.EsDoc;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.bulk.BulkProcessor.Builder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class WriterApi {

    public static final Gson gson = (new GsonBuilder()).serializeNulls()
        .enableComplexMapKeySerialization().create();

    private static final Logger LOG = Logger.getLogger(WriterApi.class);

    private final String indexName;
    private final String indexType;
    private final Integer bulkActions;
    private final TimeValue flushInterval;
    private final EsClient client;
    private final BulkProcessor bulkProcessor;
    private BulkProcessor.Listener listener;

    /**
     * 是否在source里面保存id字段，需求见：COMM-34.
     */
    private boolean keepId = true;

    private String idField = "id";

    /**
     * bulk request的默认超时时间，跟官方的默认时间保持一致
     */
    private final TimeValue timeout = new TimeValue(1, TimeUnit.MINUTES);

    //如果不指定listener， 则使用 默认listener
    final private BulkProcessor.Listener defaultListener = new BulkProcessor.Listener() {
        public void beforeBulk(long executionId, BulkRequest request) {
            request.timeout(timeout);
            LOG.info("Going to execute new bulk composed of {} actions:" + request.numberOfActions()
                + ", size : " + request.estimatedSizeInBytes());
        }

        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            int succCnt = 0;
            int i = 0;
            List list = new ArrayList(request.requests());
            BulkItemResponse[] arr$ = response.getItems();
            int len$ = arr$.length;

            for (int i$ = 0; i$ < len$; ++i$) {
                BulkItemResponse bir = arr$[i$];
                if (bir.isFailed()) {
                    Throwable e = bir.getFailure().getCause();
                    LOG.error(e.getMessage(), e);
                    if (canRetry(e)) {
                        //为了避免跨版本的编译错误~
                        if (list.get(i) instanceof IndexRequest) {
                            WriterApi.this.bulkProcessor.add((IndexRequest) list.get(i));
                        }
                        if (list.get(i) instanceof DeleteRequest) {
                            WriterApi.this.bulkProcessor.add((DeleteRequest) list.get(i));
                        }
                    }
                } else {
                    ++succCnt;
                }

                ++i;
            }

            LOG.info(
                "Executed bulk composed of {} actions:" + request.numberOfActions() + ", size : "
                    + request.estimatedSizeInBytes() + ", succCnt : " + succCnt);
        }

        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            LOG.warn("Error executing bulk", failure);
        }
    };


    public WriterApi(EsClient esClient, String indexName, String indexType) {
        this(esClient, indexName, indexType, 1000, null);
    }

    public WriterApi(EsClient esClient, String indexName, String indexType, Integer bulkActions) {
        this(esClient, indexName, indexType, bulkActions, null);
    }

    public WriterApi(EsClient esClient, String indexName, String indexType, int bulkActions,
        TimeValue flushInterval) {
        this(esClient, indexName, indexType, bulkActions, flushInterval, null);
    }

    public WriterApi(EsClient esClient, String indexName, String indexType, int bulkActions,
        TimeValue flushInterval, BulkProcessor.Listener listener) {
        this.client = esClient;
        this.indexName = indexName;
        this.indexType = indexType;
        this.bulkActions = bulkActions;
        this.flushInterval = flushInterval;
        this.listener = listener;
        this.bulkProcessor = this.createBulkProcessor();
    }

    //----------------------------添加文档到ES-------------------------------

    /**
     * @param doc 必须包含id属性。
     */
    public void addDoc(Map<String, Object> doc) throws IOException {
        this.addDoc(doc, (String) null);
    }

    public void addDoc(EsDoc esDoc) throws IOException {
        this.addDoc(esDoc.getDataMap(), (String) null);
    }


    public void addDoc(Map<String, Object> doc, String parent) throws IOException {
        this.bulkProcessor.add(buildIndexRequest(doc, parent));
    }

    public void addDoc(Map<String, Object> doc, AddDocOpType opType) {
        this.addDoc(doc, opType, null);
    }

    public void addDoc(Map<String, Object> doc, AddDocOpType opType, String parent) {
        IndexRequest indexRequest = buildIndexRequest(doc, parent);
        if (opType != null) {
            indexRequest.opType(opType.toString());
        }
        this.bulkProcessor.add(indexRequest);
    }

    private IndexRequest buildIndexRequest(Map<String, Object> doc, String parent) {
        Preconditions.checkNotNull(doc, "the doc map can not null!");
        Preconditions.checkArgument(doc.containsKey(idField), "the element id can not null!");

        IndexRequest indexRequest = Requests.indexRequest(this.indexName)
            .type(this.indexType)
            .id(getId(doc));

        if (parent != null) {
            indexRequest.parent(parent);
        }

        removeIdIfNeed(doc);
        indexRequest.source(doc);

        return indexRequest;
    }

    private UpdateRequest buildUpdateRequest(Map<String, Object> doc, String parent) {
        Preconditions.checkNotNull(doc, "the doc map can not null!");
        Preconditions.checkArgument(doc.containsKey(idField), "the element id can not null!");

        UpdateRequest updateRequest = new UpdateRequest(indexName, indexType, getId(doc));
        if (parent != null) {
            updateRequest.parent(parent);
        }

        removeIdIfNeed(doc);
        updateRequest.doc(doc);

        return updateRequest;
    }

    //---------------------------------------------------------------------------------------

    //--------------------------------------删除ES文档----------------------------------------
    public boolean deleteDoc(String id) {
        this.deleteDoc(id, null);
        return true;
    }

    public boolean deleteDoc(String id, String parent) {
        Preconditions.checkNotNull(id, "the element id can not null!");

        DeleteRequest deleteRequest = Requests.deleteRequest(this.indexName).type(this.indexType)
            .id(id);
        if (parent != null) {
            deleteRequest.parent(parent);
        }

        //TODO: 5.6.0的highlevel API 不支持 index操作
//        RefreshRequest refreshRequest = Requests.refreshRequest(new String[]{this.indexName});
//        this.client.getClient().admin().indices().refresh(refreshRequest).actionGet();

        this.bulkProcessor.add(deleteRequest);
        return true;
    }
    //---------------------------------------------------------------------------------------


    //--------------------------------------局部更新ES文档----------------------------------------
    public void updateDoc(Map<String, Object> doc) throws IOException {
        this.updateDoc(doc, null);
    }

    public void updateDoc(Map<String, Object> doc, String parent) throws IOException {
        this.bulkProcessor.add(buildUpdateRequest(doc, parent));
    }
    //---------------------------------------------------------------------------------------

    /**
     * upsert，默认情况下，插入和更新的doc内容一致
     * @param doc
     * @param parent
     */
    public void upsertDoc(Map<String, Object> doc, String parent) {
        upsertDoc(doc, doc, parent);
    }

    /**
     *
     * @param insertDoc docId不存在时，插入的doc
     * @param updateDoc docId存在时，更新的doc
     * @param parent
     */
    public void upsertDoc(Map<String, Object> insertDoc, Map<String, Object> updateDoc,
        String parent) {
        IndexRequest indexRequest = buildIndexRequest(insertDoc, parent);
        UpdateRequest updateRequest = buildUpdateRequest(updateDoc, parent)
            .upsert(indexRequest);
        this.bulkProcessor.add(updateRequest);
    }

    public void close() {
        while (true) {
            try {
                // 等待时间必须比request的超时时间长，见COMM-18.
                boolean b = this.bulkProcessor
                    .awaitClose(timeout.getSeconds() + 5, TimeUnit.SECONDS);
                if (!b) {
                    continue;
                }

                this.client.close();
            } catch (Exception var2) {
                LOG.error(var2.getMessage(), var2);
            }

            return;
        }
    }

    public void flush() {
        this.bulkProcessor.flush();
    }

    private BulkProcessor createBulkProcessor() {
        ThreadPool threadPool = new ThreadPool(client.getSettings());
        if (listener == null) {
            listener = defaultListener;
        }

        //v5.6.0
        Builder builder = new Builder(client.getClient()::bulkAsync, listener, threadPool);

        //v6.3.0
//        Builder builder = BulkProcessor.builder(client.getClient()::bulkAsync, defaultListener);

        if (this.bulkActions != 0) {
            builder.setBulkActions(this.bulkActions);
        }

        if (this.flushInterval != null) {
            builder.setFlushInterval(this.flushInterval);
        }

        builder.setBackoffPolicy(BackoffPolicy.exponentialBackoff());
        return builder.build();
    }

    public static boolean canRetry(Throwable e) {
        boolean flag = e instanceof EsRejectedExecutionException;
        if (e instanceof RemoteTransportException) {
            flag |= e.getMessage().contains(EsRejectedExecutionException.class.getSimpleName());
        }

        return flag;
    }

    public String toJson(Map<String, Object> doc) throws IOException {
        return gson.toJson(doc);
    }

    private String getId(Map<String, Object> doc) throws NullPointerException {
        return doc.get(idField).toString();
    }

    public BulkProcessor.Listener getListener() {
        return listener;
    }

    public void setListener(BulkProcessor.Listener listener) {
        this.listener = listener;
    }

    public WriterApi setKeepId(boolean keep) {
        this.keepId = keep;
        return this;
    }

    public WriterApi setIdField(String idField) {
        this.idField = idField;
        return this;
    }

    private void removeIdIfNeed(Map<String, Object> doc) {
        if (keepId || doc == null) {
            return;
        }

        if (doc.containsKey("id")) {
            doc.remove("id");
        }

        if (doc.containsKey("_id")) {
            doc.remove("_id");
        }
    }
}
