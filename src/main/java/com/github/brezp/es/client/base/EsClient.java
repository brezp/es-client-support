package com.github.brezp.es.client.base;


import com.github.brezp.es.client.entity.EsVersion;
import com.github.brezp.es.client.filter.AbstractRequestFilter;
import com.github.brezp.es.client.filter.AggFilter;
import com.github.brezp.es.client.filter.ScriptFilter;
import com.github.brezp.es.client.filter.ScrollFilter;
import com.google.common.base.Strings;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.log4j.Logger;
import org.elasticsearch.client.RestClientProxy;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Description: 基于ES官方highlevel api接口的EsClient
 *
 * @author: abel.chan, sugan
 * @since: 2018-02-24
 */
public class EsClient {

    private static final Logger LOG = Logger.getLogger(EsClient.class);

    protected RestClientProxy client;
    protected RestClient lowclient;
    private boolean reuse;
    private Settings settings;
    private String cacheKey;

    public RestClientProxy getClient() {
        if (reuse) {
            //激活缓存，避免过期
            Builder.cacheStrategy.getCache().active(cacheKey);
        }
        return client;
    }

    public void close() {
        if (client != null && !reuse) {
            try {
                lowclient.close();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    public Settings getSettings() {
        return settings;
    }

    public static class Builder {

        private static CacheStrategy cacheStrategy = CacheStrategy.ALWAYS; //只记录缓存策略，不引用实际缓存，允许用户配置后再初始化（Lazy）

        public static void setCacheStrategy(CacheStrategy cacheStrategy) {
            Builder.cacheStrategy = cacheStrategy;
        }

        protected RestClientProxy client = null;

        private Settings settings = null;
        private EsVersion esVersion = EsVersion.DEFAULT;
        private boolean reuseClient = true;//默认开启，让连接可复用

        private String cluster = null;
        private String[] esHosts;
        private int httpPort = 9200;
        private int maxRetryTimeoutMillis = 120 * 1000;

        /**
         * apache http client的socket超时时间，默认120s，请确保该超时时间大于reader api or writer api所设置的
         * 超时时间，否则可能会抛出java.net.SocketTimeoutException的异常
         */
        private int clientSocketTimeoutMillis = 120 * 1000;

        // SearchGuard 账号密码认证
        private String username = null;
        private String password = null;

        private List<AbstractRequestFilter> filters = new LinkedList<>();
        private List<AbstractRequestFilter> defaultFilters = new LinkedList<>();

        public Builder() {
            //初始化默认filter
            defaultFilters.add(new AggFilter());
            defaultFilters.add(new ScrollFilter());
            defaultFilters.add(new ScriptFilter());
        }

        //添加自定义filter
        public void setFilters(List<AbstractRequestFilter> filters) {
            this.filters = filters;
        }

        public Builder setClient(RestClientProxy client) {
            this.client = client;
            return this;
        }

        public Builder setSettings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder setMaxTimeout(int maxRetryTimeoutMillis) {
            this.maxRetryTimeoutMillis = maxRetryTimeoutMillis;
            return this;
        }

        public Builder setClientSocketTimeoutMillis(int clientSocketTimeoutMillis) {
            this.clientSocketTimeoutMillis = clientSocketTimeoutMillis;
            return this;
        }

        public Builder setCluster(String cluster) {
            this.cluster = cluster;
            return this;
        }

        public Builder setEsHosts(String[] esHosts) {
            this.esHosts = esHosts;
            return this;
        }

        public Builder setEsVersion(EsVersion esVersion) {
            this.esVersion = esVersion;
            return this;
        }

        /**
         * 是否需要复用LowLevelClient，在需要复用LowLevelClient的情况下，close client不生效
         *
         * @param reuseClient，默认false
         * @return
         */
        public Builder setReuseClient(boolean reuseClient) {
            this.reuseClient = reuseClient;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        private String clientKey(HttpHost[] httpHosts, String username, String password) {
            return Arrays.stream(httpHosts)
                .map(HttpHost::toHostString)
                .collect(Collectors.joining(",")) + "," + username + "," + password;
        }


        public EsClient build() throws Exception {
            EsClient client = new EsClient();

            if (settings == null) {
                settings = Settings.builder()
                    .put("cluster.name", cluster)
                    .put("client.transport.ping_timeout", "60s")
                    .build();

                InetAddress IP = InetAddress.getLocalHost();

                LOG.info("nodeID : \t" + IP.getHostName());

                /**
                 * 保证 nodeId 的字符串长度大于7，不然报错!
                 */
                settings = Node
                    .addNodeNameIfNeeded(settings, IP.getHostName() + UUID.randomUUID().toString());
            }

            client.setSettings(settings);

            HttpHost[] httpHosts = initHosts(esHosts, httpPort);
            String clientCacheKey = clientKey(httpHosts, username, password);
            client.cacheKey = clientCacheKey;
            if (this.client != null) {
                client.client = this.client;
            } else {
                if (esHosts != null) {
                    client.lowclient = reuseClient
                        ? getCacheLowLevelClient(clientCacheKey, httpHosts, username, password)
                        : getLowLevelClient(httpHosts, username, password);
                    client.client = new RestClientProxy(client.lowclient, this.esVersion);
                    client.client.addFilters(defaultFilters);
                    client.client.addFilters(filters);
                    client.reuse = reuseClient;
                } else {
                    throw new Exception("error esHosts");
                }
            }
            return client;
        }

        public static HttpHost[] initHosts(String[] esHosts, int port) {
            HttpHost[] httpHosts = new HttpHost[esHosts.length];
            for (int i = 0; i < esHosts.length; i++) {
                int p = port;
                String[] parts = esHosts[i].split(":");
                if (parts.length > 1) {
                    p = Integer.parseInt(parts[1]);
                }
                httpHosts[i] = new HttpHost(parts[0], p);
            }
            return httpHosts;
        }

        // 加入SearchGuard 账号密码认证
        private synchronized RestClient getCacheLowLevelClient(String clientCacheKey,
            HttpHost[] httpHosts, String username, String password) {
            RestClientCache cachedClients = cacheStrategy.getCache(); //Enum真正实例化
            if (cachedClients.containsKey(clientCacheKey)) {
                return cachedClients.get(clientCacheKey);
            }

            RestClient restClient = authEnabled() ? getLowLevelClient(httpHosts, username, password)
                : getLowLevelClient(httpHosts);
            cachedClients.put(clientCacheKey, restClient);
            return restClient;
        }

        private RestClient getLowLevelClient(HttpHost[] httpHosts) {
            RestClientBuilder builder = RestClient.builder(httpHosts);
            builder.setMaxRetryTimeoutMillis(maxRetryTimeoutMillis);
            addTimeout(builder);

            return builder.build();
        }

        // 加入SearchGuard 账号密码认证
        private RestClient getLowLevelClient(HttpHost[] httpHosts, String username,
            String password) {

            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider
                .setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

            RestClientBuilder builder = RestClient.builder(httpHosts);
            builder.setMaxRetryTimeoutMillis(maxRetryTimeoutMillis);
            builder.setHttpClientConfigCallback(httpClientBuilder -> {
                httpClientBuilder.disableAuthCaching();
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            });
            addTimeout(builder);

            return builder.build();
        }

        private boolean authEnabled() {
            return !Strings.isNullOrEmpty(username) && !Strings.isNullOrEmpty(password);
        }

        private void addTimeout(RestClientBuilder clientBuilder) {
            clientBuilder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                .setSocketTimeout(clientSocketTimeoutMillis)
            );
        }
    }

    private void setSettings(Settings settings) {
        this.settings = settings;
    }
}
