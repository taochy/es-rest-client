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

  public UnifiedGetRequest(EsClient client,String indexName, String docId){
    super(indexName,docId);
    this.rhlClient = client.getClient();
  }
}
