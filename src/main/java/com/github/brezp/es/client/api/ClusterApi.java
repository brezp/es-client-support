package com.github.brezp.es.client.api;

import com.github.brezp.es.client.base.EsClient;
import org.apache.log4j.Logger;
import org.elasticsearch.action.main.MainResponse;

/**
 * Created by turner on 2018/7/11.
 */
public class ClusterApi {

    private static Logger LOG = Logger.getLogger(ClusterApi.class);

    private EsClient client;

    public ClusterApi(EsClient esClient) {
        this.client = esClient;
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

    /**
     * 测试连通性
     * @return
     */
    public boolean ping(){
        try {
            return this.client.getClient().ping();
        }catch (Exception e){
        }
        return false;
    }

    /**
     * 集群信息
     * @return
     */
    public MainResponse info(){
        try {
            return this.client.getClient().info();
        }catch (Exception e){
            LOG.error(e.getMessage(),e);
        }
        return null;
    }


}
