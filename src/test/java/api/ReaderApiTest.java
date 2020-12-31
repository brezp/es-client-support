package api;

import com.github.brezp.es.client.api.ReaderApi;
import com.github.brezp.es.client.base.EsClient;
import com.github.brezp.es.client.entity.EsReaderResult;
import org.apache.log4j.Logger;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.shaded.org.apache.commons.lang.time.StopWatch;

import java.util.concurrent.*;

/**
 * 测试ReaderApi
 *
 * @author: abel.chan
 * @since: 2018-02-24
 * @time: 上午11:57
 */
public class ReaderApiTest {

    private static Logger LOG = Logger.getLogger(ReaderApiTest.class);


    private static ReaderApi readerApi;

    private static EsClient client;

    @Before
    public void init() throws Exception {
        EsClient client = new EsClient.Builder()
                .setCluster("tencent_es_cluster")
                .setEsHosts(new String[]{"dev3:9209"})
                .build();
        this.client = client;
        boolean ping = client.getClient().ping();
        System.out.println(ping);
        ReaderApi readerApi = new ReaderApi(client, "ds-banyan-video-index-v1", "comment");
        this.readerApi = readerApi;
    }

    @After
    public void destroy() {
        client.close();
    }

    @Test
    public void testSearch() throws Exception {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        QueryBuilder queryBuilder = QueryBuilders.termQuery("is_robot", "0");
        ExecutorService executorService = Executors.newFixedThreadPool(8);
        for (int i = 0; i < 100; i++) {
            int finalI = i;
            executorService.execute(() -> {
                try {
                    EsReaderResult esReaderResult = readerApi.search(0, finalI, queryBuilder);
                    System.out.println(Thread.currentThread().getId() + "EsReaderResult:" + esReaderResult.getDataSize());
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            });
        }
        System.out.println("here here !!!!!!1");
        executorService.awaitTermination(5, TimeUnit.MINUTES);

    }


}
