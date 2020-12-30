package com.github.brezp.es.client.api;



import com.github.brezp.es.client.util.GetAliasesSpecificName;
import com.github.brezp.es.client.util.JsonPathUtil;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.searchbox.action.Action;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.client.http.JestHttpClient;
import io.searchbox.indices.*;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import org.apache.log4j.Logger;

import java.util.*;


/**
 * @author brezp
 */
public class IndexApi {

    private static final Logger LOG = Logger.getLogger(IndexApi.class);
    protected final JestClientFactory factory = new JestClientFactory();
    protected JestHttpClient client;


    public IndexApi(String[] esHosts) throws Exception {
        this(esHosts, null, null);
    }

    public IndexApi(String[] esHosts, String username, String password) throws Exception {
        String[] temp = new String[esHosts.length];
        for (int i = 0; i < esHosts.length; i++) {
            temp[i] = "http://" + esHosts[i];
        }
        this.setUp(temp, username, password);
    }

    public void setUp(String[] esHosts) throws Exception {
        setUp(esHosts, null, null);
    }

    public void setUp(String[] esHosts, String username, String password) throws Exception {
        HttpClientConfig.Builder builder = new HttpClientConfig
                .Builder(Arrays.asList(esHosts))
                .readTimeout(10000)
                .multiThreaded(true);
        if (!Strings.isNullOrEmpty(username) && !Strings.isNullOrEmpty(password)) {
            builder.defaultCredentials(username, password);
        }
        factory.setHttpClientConfig(builder.build());
        client = (JestHttpClient) factory.getObject();
    }


    public void tearDown() throws Exception {
        client.close();
        client = null;
    }

    /**
     * TODO
     * es 5.6 不兼容问题
     */
    public boolean createIndex(String indexName, String indexType, Object settings, String source) {
        JestResult jestResult = null;
        if (!indexExists(indexName)) {
            jestResult = executeWrapper(
                new CreateIndex.Builder(indexName).settings(settings).build(), true
            );
        }

        if (jestResult == null || jestResult.isSucceeded()) {
            jestResult = executeWrapper(
                new PutMapping.Builder(indexName, indexType, source).build(), true
            );
        }

        return jestResult.isSucceeded();
    }

    private void printError(String uri, JestResult jestResult, boolean logError) {
        if (!jestResult.isSucceeded() && logError) {
            LOG.warn(
                String.format("Request URI: %s, error: %s", uri, jestResult.getErrorMessage()));
        }
    }

    public boolean indexExists(String index) {
        return executeWrapper(
            new IndicesExists.Builder(index).build(), false
        ).isSucceeded();
    }

    public boolean open(String index) {
        return executeWrapper(
            new OpenIndex.Builder(index).build(), true
        ).isSucceeded();
    }


    public boolean close(String index) {
        return executeWrapper(
            new CloseIndex.Builder(index).build(), true
        ).isSucceeded();
    }


    public boolean deleteIndex(String index) {
        return executeWrapper(
            new DeleteIndex.Builder(index).build(), true
        ).isSucceeded();
    }

    public List<Object> indexList() {
        JestResult jestResult = executeWrapper(
            new Stats.Builder().build(), true
        );

        if (jestResult.isSucceeded()) {
            try {
                return new ArrayList<>(
                    jestResult.getJsonObject().getAsJsonObject("indices").keySet()
                );
            } catch (Exception e) {
                LOG.error(jestResult.getJsonString(), e);
            }
        }

        return null;
    }

    public Set<String> getIndexbyAliases(String aliases) {
        JestResult jestResult = executeWrapper(
            new GetAliasesSpecificName.Builder().alias(aliases).build(), true
        );

        if (jestResult.isSucceeded()) {
            return jestResult.getJsonMap().keySet();
        }

        return new HashSet<>();
    }

    public Map<String, Object> getSchema(String index, String type) {
        JestResult jestResult = executeWrapper(
            new GetMapping.Builder().addIndex(getRealName(index)).build(), true
        );

        Map<String, Object> map = new HashMap<>();
        if (jestResult.isSucceeded()) {
            try {
                JsonObject indexMapping = jestResult.getJsonObject()
                    .getAsJsonObject(getRealName(index))
                    .getAsJsonObject("mappings");
                JsonObject typeProperties;
                if (!indexMapping.has(type)) {
                    typeProperties = indexMapping.getAsJsonObject("properties");
                } else {
                    typeProperties = indexMapping.getAsJsonObject(type)
                            .getAsJsonObject("properties");
                }

                if(typeProperties != null && !typeProperties.isJsonNull()) {
                    for (String key : typeProperties.keySet()) {
                        map.put(key, typeProperties.get(key));
                    }
                }
            } catch (Exception e) {
                LOG.error(jestResult.getJsonString(), e);
            }
        }
        return map;
    }

    /**
     * @param index 索引名
     * @return 存储大小的map eg: {"size": "348.6gb", "size_in_bytes": 374321659489, "throttle_time":
     * "0s", "throttle_time_in_millis": 0 }
     */
    public Map<String, Object> statSize(String index) {
        JestResult jestResult = executeWrapper(
            new Stats.Builder().addIndex(index).setParameter("human", "1").build(), true
        );

        if (jestResult.isSucceeded()) {
            try {
                String json = JsonPathUtil
                    .read(jestResult.getJsonString(), "$._all.primaries.store");
                return new HashMap<>(JsonPathUtil.str2map(json));
            } catch (Exception e) {
                LOG.error(jestResult.getJsonString(), e);
            }
        }

        return null;
    }

    /**
     * 默认取最后一个字典序的索引名
     * @param index
     * @return
     */
    private String getRealName(String index) {
        Set<String> indexSet = getIndexbyAliases(index);
        if (indexSet != null && !indexSet.isEmpty()) {
            List<String> sortedIndex = new ArrayList<>(indexSet);
            sortedIndex.sort(Comparator.reverseOrder());
            return sortedIndex.get(0);
        }
        return index;
    }

    private <T extends JestResult> JestResult executeWrapper(Action<T> clientRequest,
        boolean logError) {
        JestResult jestResult = new JestResult(new Gson()
            .fromJson("{\"isSucceeded\":false,\"errorMessage\":\"\"}", JestResult.class));
        try {
            jestResult = client.execute(clientRequest);
            printError(clientRequest.getURI(), jestResult, logError);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            jestResult.setErrorMessage(e.getMessage());
        }

        return jestResult;
    }
}
