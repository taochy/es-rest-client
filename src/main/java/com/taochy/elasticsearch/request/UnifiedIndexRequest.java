package com.taochy.elasticsearch.request;

import com.taochy.elasticsearch.Util.ESPubPara;
import com.taochy.elasticsearch.client.EsClient;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @author ：taochy
 * @date ：Created in 2020/12/23 11:17 上午
 * @description：扩展的IndexRequest，留待后续功能扩展
 * @modified By：
 * @version: 1.0.0.0
 */
public class UnifiedIndexRequest extends IndexRequest {

    private RestHighLevelClient rhlClient;

    public UnifiedIndexRequest(EsClient client, String indexName) {
        super(indexName);
        this.rhlClient = client.getClient();
        if (client.isHasType()) {
            this.type(ESPubPara.ES_TYPE);
        } else {
            this.type(ESPubPara.ES_TYPE_DOC);
        }
    }
}
