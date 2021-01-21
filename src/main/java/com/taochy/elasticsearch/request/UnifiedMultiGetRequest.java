package com.taochy.elasticsearch.request;

import com.taochy.elasticsearch.client.EsClient;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @author ：taochy
 * @date ：Created in 2020/12/23 2:31 下午
 * @description：扩展的MultiGetRequest，留待后续功能扩展
 * @modified By：
 * @version: 1.0.0.0
 */
public class UnifiedMultiGetRequest extends MultiGetRequest {
  private RestHighLevelClient rhlClient;

  public UnifiedMultiGetRequest(EsClient client){
    super();
    this.rhlClient = client.getClient();
  }

}
