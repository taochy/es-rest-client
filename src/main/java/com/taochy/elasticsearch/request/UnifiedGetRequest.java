package com.taochy.elasticsearch.request;

import com.taochy.elasticsearch.client.EsClient;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @author ：taochy
 * @date ：Created in 2020/12/23 11:01 上午
 * @description：扩展的getRequest，留待后续功能扩展
 * @modified By：
 * @version: 1.0.0.0
 */
public class UnifiedGetRequest extends GetRequest {

    private RestHighLevelClient rhlClient;

    public UnifiedGetRequest(EsClient client, String indexName, String docId) {
        super(indexName);
        //由于6.X版本仅有一个type并且在7.X及以后版本已经取消了type，所以在读数据时将type属性取消，兼容所有版本
//    this.type(ESPubPara.ES_TYPE);
        this.id(docId);
        this.rhlClient = client.getClient();
    }
}
