package com.github.brezp.es.client.base;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 缓存策略
 *
 */
public enum CacheStrategy {
    ALWAYS { //全部一直缓存

        @Override
        RestClientCache getCache() {
            return RestClientAlwaysCache.INSTANCE;
        }
    },
    LRU { //LRU

        @Override
        RestClientCache getCache() {
            return RestClientLRUCache.INSTANCE;
        }
    },
    WTinyLFU { //Window TinyLfu 回收策略

        @Override
        RestClientCache getCache() {
            return RestClientWTinyLFUCache.INSTANCE;
        }
    },
    ;

    public static int EXPIRE_MINUTE = 60;

    /**
     * RestClientCache不作为成员变量，保证lazy
     */
    abstract RestClientCache getCache();
}

/**
 * cache统一接口，兼容Map和guava的Cache
 *
 * @author Leibniz on 2019/10/29 11:57 上午
 */
interface RestClientCache {
    boolean containsKey(String key);

    RestClient get(String key);

    void put(String key, RestClient value);

    void active(String key);
}

/**
 * RestClientCache的hashMap实现
 *
 * @author Leibniz on 2019/10/29 11:57 上午
 */
enum RestClientAlwaysCache implements RestClientCache {
    INSTANCE;

    private final Map<String, RestClient> cachedClients = new HashMap<>();

    @Override
    public boolean containsKey(String key) {
        return cachedClients.containsKey(key);
    }

    @Override
    public RestClient get(String key) {
        return cachedClients.get(key);
    }

    @Override
    public void put(String key, RestClient value) {
        cachedClients.put(key, value);
    }

    @Override
    public void active(String key) {
    }
}

/**
 * RestClientCache的Guava LRU Cache实现
 *
 * @author Leibniz on 2019/10/29 11:57 上午
 */
enum RestClientLRUCache implements RestClientCache {
    INSTANCE;
    private final Cache<String, RestClient> cachedClients = CacheBuilder.newBuilder()
            .expireAfterAccess(CacheStrategy.EXPIRE_MINUTE, TimeUnit.MINUTES)
            .removalListener(new ESConnectorListener())
            .build();

    @Override
    public boolean containsKey(String key) {
        return cachedClients.getIfPresent(key) != null;
    }

    @Override
    public RestClient get(String key) {
        return cachedClients.getIfPresent(key);
    }

    @Override
    public void put(String key, RestClient value) {
        cachedClients.put(key, value);
    }

    @Override
    public void active(String key) {
        get(key);
    }

    /**
     * 过期RestClient移除的监听器
     *
     * @author Leibniz on 2019/10/29 11:57 上午
     */
    static class ESConnectorListener implements RemovalListener<String, Object> {
        private final Logger LOG = LoggerFactory.getLogger(getClass());

        @Override
        public void onRemoval(RemovalNotification<String, Object> notification) {
            Object client = notification.getValue();
            if (client instanceof RestClient) {
                LOG.info("LRU cache strategy remove a RestClient:{}", client);
                try {
                    ((RestClient) client).close();
                } catch (IOException e) {
                    LOG.error("close es client error.", e);
                }
            }
        }
    }
}

/**
 * RestClientCache的Window TinyLFU实现
 * caffeine 依赖的scope为provided，需要使用方手动加入
 *
 * @author Leibniz on 2019/10/29 11:57 上午
 */
enum RestClientWTinyLFUCache implements RestClientCache {
    INSTANCE;

    private final com.github.benmanes.caffeine.cache.Cache<String, RestClient> cachedClients = Caffeine.newBuilder()
            .expireAfterAccess(CacheStrategy.EXPIRE_MINUTE, TimeUnit.MINUTES)
            .removalListener(new ESConnectorListener())
            .build();

    @Override
    public boolean containsKey(String key) {
        return cachedClients.getIfPresent(key) != null;
    }

    @Override
    public RestClient get(String key) {
        return cachedClients.getIfPresent(key);
    }

    @Override
    public void put(String key, RestClient value) {
        cachedClients.put(key, value);
    }

    @Override
    public void active(String key) {
        get(key);
    }

    /**
     * 过期RestClient移除的监听器
     *
     * @author Leibniz on 2019/10/29 11:57 上午
     */
    static class ESConnectorListener implements com.github.benmanes.caffeine.cache.RemovalListener<String, Object> {
        private final Logger LOG = LoggerFactory.getLogger(getClass());

        @Override
        public void onRemoval(@Nullable String s, @Nullable Object client, @NonNull RemovalCause removalCause) {
            if (client instanceof RestClient) {
                LOG.info("Window-TinyLFU cache strategy remove a RestClient:{}", client);
                try {
                    ((RestClient) client).close();
                } catch (IOException e) {
                    LOG.error("close es client error.", e);
                }
            }
        }
    }
}
