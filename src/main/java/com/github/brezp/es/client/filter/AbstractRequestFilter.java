package com.github.brezp.es.client.filter;

import com.github.brezp.es.client.entity.EsVersion;
import com.google.common.base.Strings;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @author sugan
 * @since 2018-07-06.
 */
public abstract class AbstractRequestFilter {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractRequestFilter.class);

    public Request filter(Request srcRequest, EsVersion version) throws IOException {

        //默认拦截request的 entity， 需要更多操作自行重载实现。

        if (skipEndpoint(srcRequest.getEndpoint())) {
            LOG.debug("skip endpoint:" + srcRequest.getEndpoint());
            return srcRequest;
        }

        HttpEntity entity = srcRequest.getEntity();
        if (entity == null) {
            return srcRequest;
        }

        String srcSource = EntityUtils.toString(entity, "utf-8");
        LOG.debug("Src Source:{}", srcSource);
        String modifiedSource = srcSource;
        Map<String, String> modifiedParameters = srcRequest.getParameters();
        switch (version) {
            case V1_7:
                modifiedSource = compatibleForV17(srcSource);
                modifiedParameters = compatibleForV17Parameters(srcRequest);
                break;
            case V2_3:
                modifiedSource = compatibleForV23(srcSource);
                modifiedParameters = compatibleForV23Parameters(srcRequest);
                break;
            case V7_9:
                modifiedSource = compatibleForV79(srcSource);
                modifiedParameters = compatibleForV79Parameters(srcRequest);
            default:
                break;
        }
        if (!Strings.isNullOrEmpty(srcSource) && Strings.isNullOrEmpty(modifiedSource)) {
            throw new RuntimeException(String.format("src source:%s, afert modified:%s", srcSource, modifiedSource));
        }

        LOG.debug("Modified Source {}", modifiedSource);

        StringEntity newEntity = new StringEntity(modifiedSource, ContentType
            .parse(entity.getContentType().getValue())
            .withCharset("utf-8")
        );

        return new Request(HttpPost.METHOD_NAME, srcRequest.getEndpoint(), modifiedParameters, newEntity);
    }

    /**
     * 匹配指定endpoint
     *
     * @param endpoint 当前request的endpoint（ 访问API路径）， 示例: /_search/scroll
     * @return false： 不跳过，  true:跳过
     */
    protected boolean skipEndpoint(String endpoint) {
        return false;
    }

    /**
     * 适配v2.3版本的json
     *
     * @param sourceJson 请求的实际json body
     * @return
     */
    protected abstract String compatibleForV23(String sourceJson);

    /**
     * 适配v7.9版本的json
     *
     * @param sourceJson 请求的实际json body
     * @return
     */
    protected abstract String compatibleForV79(String sourceJson);

    /**
     * 适配v1.7版本的json
     *
     * @param sourceJson 请求的实际json body
     * @return
     */
    protected abstract String compatibleForV17(String sourceJson);

    protected Map<String, String> compatibleForV17Parameters(Request srcRequest) throws IOException {
        return srcRequest.getParameters();
    }

    protected Map<String, String> compatibleForV23Parameters(Request srcRequest) {
        return srcRequest.getParameters();
    }

    protected Map<String, String> compatibleForV79Parameters(Request srcRequest) {
        return srcRequest.getParameters();
    }

    public String getName() {
        return this.getClass().getSimpleName();
    }
}
