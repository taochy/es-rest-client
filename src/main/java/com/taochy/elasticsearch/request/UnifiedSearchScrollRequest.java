package com.taochy.elasticsearch.request;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;

/**
 * @author ：taochy
 * @date ：Created in 2020/12/22 5:24 下午
 * @description：扩展的searchscroll request，留待后续功能扩展
 * @modified By：
 * @version: 1.0.0.0
 */

@Slf4j
public class UnifiedSearchScrollRequest extends SearchScrollRequest {

  private RestHighLevelClient rhlClient;
  private SearchResponse originResponse;

  public UnifiedSearchScrollRequest(RestHighLevelClient rhlClient,String scrollId){
    super(scrollId);
    this.rhlClient = rhlClient;
  }

  public UnifiedSearchScrollRequest(RestHighLevelClient rhlClient,SearchResponse originResponse){
    super(originResponse.getScrollId());
    this.originResponse = originResponse;
    this.rhlClient = rhlClient;
    this.setScroll(TimeValue.timeValueMinutes(1L));
  }

  public UnifiedSearchScrollRequest setScroll(TimeValue timeValue){
    Scroll scroll = new Scroll(timeValue);
    this.scroll(scroll);
    return this;
  }

  public SearchResponse get(){
    SearchResponse response = null;
    try {
      response = rhlClient.scroll(this, RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error(e.getMessage());
    }
    return response;
  }

}
