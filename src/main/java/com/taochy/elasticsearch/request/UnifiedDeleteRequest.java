package com.taochy.elasticsearch.request;

import com.taochy.elasticsearch.client.EsClient;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @author ：taochy
 * @date ：Created in 2020/12/23 11:15 上午
 * @description：扩展的deleteRequest,留待后续扩展功能
 * @modified By：
 * @version: 1.0.0.0
 */
@Slf4j
public class UnifiedDeleteRequest extends DeleteRequest {
  private RestHighLevelClient rhlClient;

  public UnifiedDeleteRequest(EsClient client, String indexName, String docId){
    super(indexName,docId);
    this.rhlClient = client.getClient();
  }
}
