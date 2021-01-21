package com.taochy.elasticsearch.request;

import com.taochy.elasticsearch.Util.ESPubPara;
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

    public UnifiedUpdateRequest(EsClient client, String indexName, String docId) {
        super();
        this.index(indexName);
        if (client.isHasType()) {
            this.type(ESPubPara.ES_TYPE);
        } else {
            this.type(ESPubPara.ES_TYPE_DOC);
        }
        this.id(docId);
    }
}
