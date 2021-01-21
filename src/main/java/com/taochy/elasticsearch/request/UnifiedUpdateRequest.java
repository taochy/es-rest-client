package com.taochy.elasticsearch.request;

import com.taochy.elasticsearch.client.EsClient;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @author ：taochy
 * @date ：Created in 2020/12/23 11:14 上午
 * @description：扩展的updateRequest，留待后续功能扩展
 * @modified By：
 * @version: 1.0.0.0
 */
public class UnifiedUpdateRequest extends UpdateRequest {
    private RestHighLevelClient rhlClient;

    public UnifiedUpdateRequest(EsClient client, String indexName, String type, String docId) {
        super(indexName, type, docId);
        this.rhlClient = client.getClient();
    }
}
