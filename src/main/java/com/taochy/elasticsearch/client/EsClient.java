package com.taochy.elasticsearch.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.taochy.elasticsearch.request.UnifiedDeleteRequest;
import com.taochy.elasticsearch.request.UnifiedGetRequest;
import com.taochy.elasticsearch.request.UnifiedIndexRequest;
import com.taochy.elasticsearch.request.UnifiedSearchRequest;
import com.taochy.elasticsearch.request.UnifiedSearchScrollRequest;
import com.taochy.elasticsearch.request.UnifiedUpdateRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

/**
 * @author ：taochy
 * @date ：Created in 2020/9/18 2:22 下午
 * @description：es-client main class
 * @modified By：
 * @version: 1.0.0.0
 */
@Slf4j
public class EsClient {

  public static boolean ES_STATUS_GREEN = false;
  private String schema = "http";
  private BulkProcessor bulkProcessor = null;
  private String hostNames;
  private int port;
  private RestClientBuilder builder;
  private RestHighLevelClient rhlClient;
  private RestClient rllClient;

  /**
   * constructor 4 hostNames
   * @param hostNames
   */
  public EsClient(String hostNames) {
    log.info("hostNames = {}", hostNames);
    this.hostNames = hostNames;
    this.port = 9200;
  }

  /**
   * constructor 4 hostNames & port
   * @param port
   * @param hostNames
   */
  public EsClient(String hostNames, int port) {
    log.info("hostNames = {}, port={}", hostNames, port);
    this.hostNames = hostNames;
    this.port = port;
  }

  /**
   * build es client.
   *
   */
  public void buildClient() throws Exception {
    if (rhlClient != null) {
      rhlClient.close();
    }
    if (rllClient != null) {
      rllClient.close();
    }

    String[] itTransportHostName = hostNames.split(",");
    HttpHost[] httpHosts = new HttpHost[itTransportHostName.length];
    log.info("init restful client");
    for (int i = 0; i < itTransportHostName.length; i++) {
      HttpHost httpHost = new HttpHost(itTransportHostName[i], port, schema);
      httpHosts[i] = httpHost;
    }
    builder = RestClient.builder(httpHosts);
    rhlClient = new RestHighLevelClient(builder);
    rllClient = rhlClient.getLowLevelClient();
    log.info("restful client created");
  }

  /**
   * build bulk processor.
   *
   */
  public void buildBulkProcessor() throws Exception {
    if (bulkProcessor != null) {
      bulkProcessor.close();
    }

    BulkProcessor.Listener listener = new BulkProcessor.Listener() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request) {
        log.debug(
            " executionId " + executionId + " numberOfActions = " + request.numberOfActions());
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
          BulkResponse response) {
        if (response.hasFailures()) {
          BulkItemResponse[] responseItems = response.getItems();
          for (BulkItemResponse item : responseItems) {
            if (item.isFailed()) {
              log.error("bulk failurs index = [{}]===id=[{}]=====message=[{}]", item.getIndex(),
                  item.getId(), item.getFailureMessage());
            }
          }
        }
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
          Throwable failure) {
        log.info("happen fail = " + failure.getMessage() + " cause = " + failure.getCause());
      }
    };

    BulkProcessor.Builder builder = BulkProcessor.builder(
        (request, bulkListener) ->
            rhlClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
        listener);
    builder.setBulkActions(1000);
    builder.setBulkSize(new ByteSizeValue(5L, ByteSizeUnit.MB));
    builder.setConcurrentRequests(3);
    builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
    builder.setBackoffPolicy(BackoffPolicy
        .constantBackoff(TimeValue.timeValueMillis(100L), 3));

    bulkProcessor = builder.build();
  }

  /**
   * close es client.
   */
  public void closeClient() {
    if (rhlClient != null) {
      try {
        rhlClient.close();
      } catch (IOException e) {
        log.info("rhlClient close failure {}", e.getMessage());
      }
    }
    if (rllClient != null) {
      try {
        rllClient.close();
      } catch (IOException e) {
        log.info("rllClient close failure {}", e.getMessage());
      }
    }
  }

  /**
   * close bulk processor.
   */
  public void closeBulkProcessor() {
    if (bulkProcessor != null) {
      //先flush再close
      bulkProcessor.flush();
      bulkProcessor.close();
    }
  }

  /**
   * delete template.
   *
   * @status verified 5&6&7
   */
  public void deleteTemplate(String strTemplateName) {
    try {
      Request request = new Request(
          "DELETE",
          "_template/" + strTemplateName);
      rllClient.performRequest(request);
    } catch (IOException e) {
      log.info("template delete failure {}", e.getMessage());
    }
  }

  /**
   * put template.
   *
   * @status verified 5&6&7
   */
  public void putTemplate(String templateName, Map<String, Object> templateSource) {
    try {
      HttpEntity entity = new NStringEntity(JSON.toJSONString(templateSource),
          ContentType.APPLICATION_JSON);
      Request request = new Request(
          "PUT",
          "_template/" + templateName);
      request.setEntity(entity);
      rllClient.performRequest(request);
    } catch (IOException e) {
      log.info("template put failure {}", e.getMessage());
    }
  }

  /**
   * check index or alias exist.
   *
   * @status verified 5&6&7
   */
  public boolean exists(String strIndex) {
    try {
      Request request = new Request(
          "HEAD",
          strIndex);
      Response response = rllClient.performRequest(request);
      boolean exist = response.getStatusLine().getReasonPhrase().equals("OK");
      return exist;
    } catch (IOException e) {
      log.info("template exist judge failure {}", e.getMessage());
    }
    return false;
  }


  //判断index是否存在
  public String[] washIndex(List<String> listIndexInput) {
    List<String> listIndexOutput = new ArrayList<>();
    for (String strIndex : listIndexInput) {
      try {
        if (exists(strIndex)) {
          listIndexOutput.add(strIndex);
        }
      } catch (Exception e) {
        log.error("", e);
      }
    }
    return listIndexOutput.toArray(new String[]{});
  }

  /**
   * 计算时段内的index后缀
   *
   * @status verified 5&6&7
   */
  public List<String> getIndexs(String strPrefix, Long startTime, Long endTime,
      String formatter) {
    List<String> listOut = new ArrayList<>();
    try {
      Request request = new Request(
          "GET",
          "_cat/indices/" + strPrefix + "*");
      Response response = rllClient.performRequest(request);
      HttpEntity entity = response.getEntity();
      InputStream content = entity.getContent();
      String line;
      BufferedReader br = new BufferedReader(new InputStreamReader(content));
      Set<String> openIndices = new HashSet<>();
      //line格式参考kibana输出
      while ((line = br.readLine()) != null) {
        openIndices.add(line.split(" ")[2]);
      }
      if(openIndices.size() > 0){
        strPrefix = strPrefix.replace("*", "");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatter);
        Date startData = new Date(startTime);
        Date endDate = new Date(endTime);
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(startData);
        while (calendar.getTime().before(endDate)) {
          String index = strPrefix + simpleDateFormat.format(calendar.getTime());
          if (openIndices.contains(index)) {
            listOut.add(index);
          }
          calendar.add(Calendar.DATE, 1);
        }
        String endIndexName = strPrefix + simpleDateFormat.format(endDate);
        if (openIndices.contains(endIndexName)) {
          listOut.add(endIndexName);
        }
        if (strPrefix.contains("dev")) {
          listOut.add(strPrefix);
        }
      }
    } catch (IOException e) {
      log.info(e.getMessage());
    }
    return listOut;
  }


  /**
   * delete index.
   *
   * @status verified 5&6&7
   */
  public void delete(String strIndex) {
    try {
      Request request = new Request(
          "DELETE",
          strIndex);
      rllClient.performRequest(request);
    } catch (IOException e) {
      log.info("index delete failure {}", e.getMessage());
    }
  }

  /**
   * create index.
   *
   * @status verified 5&6&7
   */
  public void create(String strIndex, int numShards, int numReplicas) {

    try {
      Map<String, Object> settings = new MapBuilder<String, Object>()
          .put("number_of_shards", 1)
          .put("number_of_replicas", 0)
          .put("refresh_interval", "10s")
          .map();
      XContentBuilder builder = XContentFactory.jsonBuilder()
          .startObject()
          .field("settings", JSON.toJSON(settings))
          .endObject();
      HttpEntity entity = new NStringEntity(builder.toString(), ContentType.APPLICATION_JSON);
      Request request = new Request(
          "PUT",
          "/" + strIndex);
      request.setEntity(entity);
      rllClient.performRequest(request);
    } catch (IOException e) {
      log.info(e.getMessage());
    }
  }

  /**
   * update mapping.
   *
   */
  public void putMapping(String strIndex, String strType, String strMapping) {
    try {
      Request request = new Request(
            "POST",
            "/" + strIndex + "/_mapping");
      HttpEntity entity = new NStringEntity(strMapping, ContentType.APPLICATION_JSON);
      request.setEntity(entity);
      rllClient
          .performRequest(request);
    } catch (IOException e) {
      log.info(e.getMessage());
    }
  }

  /**
   * init client and bulk processor.
   */
  private void esInit() {
    try {
      buildClient();
      buildBulkProcessor();
    } catch (Exception e) {
      log.error("", e);
    }
  }

  /**
   * client keep alive.
   *
   */
  public void keepAlive() {
    new Thread() {
      @Override
      public void run() {
        Request request = null;
        while (true) {
          log.info("===== es keep alive 1");
          if (rhlClient == null || rllClient == null) {
            esInit();
          }
          try {
            request = new Request(
                "GET",
                "_cat/nodes");
            Response nodeResponse = rllClient.performRequest(request);
            HttpEntity nodeEntity = nodeResponse.getEntity();
            InputStream nodeContent = nodeEntity.getContent();
            String nodeLine;
            BufferedReader nodeBr = new BufferedReader(new InputStreamReader(nodeContent));
            List<String> nodeList = new ArrayList<>();
            //line格式参考kibana输出
            while ((nodeLine = nodeBr.readLine()) != null) {
              nodeList.add(nodeLine.split(" ")[0]);
            }
            if (nodeList.isEmpty()) {
              System.out.println("No nodes available. Verify ES is running!");
              ES_STATUS_GREEN = false;
              log.info("===== es keep alive 2 status: {}", ES_STATUS_GREEN);
              Thread.sleep(1000 * 10);
              continue;
            } else {
              request = new Request(
                  "GET",
                  "_cluster/health");
              Response healthResponse = rllClient.performRequest(request);
              HttpEntity healthEntity = healthResponse.getEntity();
              InputStream healthContent = healthEntity.getContent();
              String healthLine;
              String status = null;
              BufferedReader healthBr = new BufferedReader(new InputStreamReader(healthContent));
              //line格式参考kibana输出
              while ((healthLine = healthBr.readLine()) != null) {
                JSONObject json = JSONObject.parseObject(healthLine);
                status = json.getString("status");
              }
              log.info("===== es keep alive name: {}", status);
              if ("green".equals(status)) {
                if (!ES_STATUS_GREEN) {
                  ES_STATUS_GREEN = true;
                  log.info("===== es keep alive 3 status: {}", ES_STATUS_GREEN);
                }
              } else {
                log.info("es status:{}", status);
                if (ES_STATUS_GREEN) {
                  ES_STATUS_GREEN = false;
                  log.info("===== es keep alive 4 status: {}", ES_STATUS_GREEN);
                }
              }
            }
          } catch (Exception e) {
            log.error("es status err", e);
            if (ES_STATUS_GREEN) {
              ES_STATUS_GREEN = false;
              log.info("===== es keep alive 5 status: {}", ES_STATUS_GREEN);
            }
            esInit();
          }
          try {
            Thread.sleep(1000 * 3);
          } catch (Exception e) {
            log.error("", e);
          }
        }
      }
    }.start();
  }

  /**
   * 得到查询游标
   *
   * @param boolQueryBuilder 条件对象
   * @param index            要查询的index，此处需要通过时间范围计算出需要查询的index
   */
  public SearchResponse getSearchResponse(BoolQueryBuilder boolQueryBuilder, int size,
      String... index) {
    SearchResponse searchResponse = null;
    try {
      SearchRequest searchRequest = new SearchRequest(index);
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.query(boolQueryBuilder);
      searchSourceBuilder.sort("server_time", SortOrder.DESC);
      searchSourceBuilder.size(size);
      searchRequest.source(searchSourceBuilder);
      searchRequest.scroll(TimeValue.timeValueMinutes(5L));
      searchRequest.searchType(SearchType.DEFAULT);
      searchResponse = rhlClient.search(searchRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.info(e.getMessage());
    }
    return searchResponse;
  }


  /**
   * 通过上一次游标得到下一次的游标
   *
   * @param searchResponse 上一次的游标
   * @return 返回下一次游标
   */
  public UnifiedSearchScrollRequest prepareSearchScroll(SearchResponse searchResponse) {
    return new UnifiedSearchScrollRequest(rhlClient,searchResponse);
  }

  /**
   * 通过上一次游标得到下一次的游标
   *
   * @param scrollId 上一次的游标Id
   * @return 返回下一次游标
   */
  public UnifiedSearchScrollRequest prepareSearchScroll(String scrollId) {
    return new UnifiedSearchScrollRequest(rhlClient,scrollId);
  }

  /**
   * 通过上一次游标得到下一次的游标
   *
   * @param searchResponse 上一次的游标
   * @return 返回下一次游标
   */
  public ClearScrollResponse prepareClearScroll(SearchResponse searchResponse) {
    ClearScrollResponse response = null;
    ClearScrollRequest request = new ClearScrollRequest();
    request.addScrollId(searchResponse.getScrollId());
    try {
      response = rhlClient.clearScroll(request, RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error(e.getMessage());
    }
    return response;
  }

  public String getTransportHostNames() {
    return hostNames;
  }

  public void setTransportHostNames(String hostNames) {
    this.hostNames = hostNames;
  }

  public RestHighLevelClient getClient() {
    return rhlClient;
  }

  public RestClient getRllClient() {
    return rllClient;
  }

  public BulkProcessor getBulkProcessor() {
    return bulkProcessor;
  }

  /**
   * alias 4 index
   *
   * @status verified 5&6&7
   */
  public void aliasIndex(String oldIndexName, String indexName) {
    if (!exists(indexName) && exists(oldIndexName)) {
      try {
        //拼接alias的restful命令体
        JSONArray array = new JSONArray();
        JSONObject actionJson = new JSONObject();
        JSONObject aliasJson = new JSONObject();
        aliasJson.fluentPut("index", oldIndexName);
        aliasJson.fluentPut("alias", indexName);
        //此处不能使用toJsonString方法，否则拼接的命令字符串会报错
        actionJson.fluentPut("add", aliasJson);
        array.add(actionJson);

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            //此处不能使用toJsonString方法，否则拼接的命令字符串会报错
            .field("actions", array)
            .endObject();
        HttpEntity entity = new NStringEntity(builder.toString(), ContentType.APPLICATION_JSON);
        Request request = new Request(
            "POST",
            "_aliases");
        request.setEntity(entity);
        rllClient.performRequest(request);
      } catch (IOException e) {
        log.info(e.getMessage());
      }
    }
  }

  /**
   * 兼容之前java代码而抽象的search方法
   * @param indexName
   * @return
   */
  public UnifiedSearchRequest prepareSearch(String... indexName){
    return new UnifiedSearchRequest(this,indexName);
  }

  /**
   * 写入数据
   *
   */
  public void prepareInsert(String indexName, String docId, Map<String, Object> jsonMap) {
    try {
      UnifiedIndexRequest indexRequest = new UnifiedIndexRequest(this,indexName);
      indexRequest.id(docId).source(jsonMap);
      indexRequest.timeout(TimeValue.timeValueSeconds(1));
      indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
      indexRequest.opType(DocWriteRequest.OpType.CREATE);
      IndexResponse indexResponse = rhlClient.index(indexRequest,RequestOptions.DEFAULT);
      System.out.println(indexResponse.getResult());
      System.out.println(indexResponse.getShardInfo().getTotal());
      System.out.println(indexResponse.getShardInfo().getFailed());
    } catch (IOException e) {
      log.error(e.getMessage());
    }
  }

  /**
   * 更新数据
   *
   */
  public void prepareUpdate(String indexName, String docId, Map<String, Object> jsonMap) {
    try {
      UnifiedUpdateRequest updateRequest = new UnifiedUpdateRequest(this,indexName, docId);
      updateRequest.doc(jsonMap);
      updateRequest.timeout(TimeValue.timeValueSeconds(1));
      updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
      UpdateResponse updateResponse = rhlClient.update(updateRequest,RequestOptions.DEFAULT);
      System.out.println(updateResponse.getResult());
      System.out.println(updateResponse.getShardInfo().getTotal());
      System.out.println(updateResponse.getShardInfo().getFailed());
    } catch (IOException e) {
      log.error(e.getMessage());
    }
  }

  /**
   * 删除数据
   *
   */
  public DeleteResponse prepareDelete(String indexName, String docId) {
    DeleteResponse deleteResponse = null;
    try {
      UnifiedDeleteRequest deleteRequest = new UnifiedDeleteRequest(this,indexName,docId);
      deleteRequest.timeout(TimeValue.timeValueSeconds(1));
      deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
      deleteResponse = rhlClient.delete(deleteRequest,RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error(e.getMessage());
    }
    return deleteResponse;
  }

  /**
   * 获取数据
   *
   */
  public GetResponse prepareGet(String indexName, String type, String docId) {
    GetResponse getResponse = null;
    try {
      UnifiedGetRequest getRequest = new UnifiedGetRequest(this, indexName, docId);
      getResponse = rhlClient.get(getRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error(e.getMessage());
    }
    return getResponse;
  }

  public static void main(String[] args) throws Exception {
    EsClient esClient = new EsClient("vm-134");
    esClient.buildClient();
    esClient.buildBulkProcessor();
    System.out.println("start!");
//    IndexRequest one = new IndexRequest("test").type("test_type").id("1")
//        .source(XContentType.JSON, "agent_id",
//            "aaa");
//    IndexRequest two = new IndexRequest("test").type("test_type").id("2")
//        .source(XContentType.JSON, "agent_id",
//            "bbb");
//    IndexRequest three = new IndexRequest("test").type("test_type").id("3")
//        .source(XContentType.JSON, "agent_id",
//            "ccc");
//    esClient.getBulkProcessor().add(one);
//    esClient.getBulkProcessor().add(two);
//    esClient.getBulkProcessor().add(three);
//    esClient.create("test", 1, 0);
//    esClient.delete("test");
//    boolean test = esClient.exists("test");
//    System.out.println(test);
//    esClient.putMapping("test","test_type","{\"dynamic\":false,\"numeric_detection\":false,\"properties\":{\"agent_id\":{\"type\":\"keyword\"}}}");
//    esClient.aliasIndex("test","test_alias");
//    esClient.putTemplate();
//    esClient.deleteTemplate("template_test");
//    List<String> indexs = esClient
//        .getIndexs("bangcle_app_", 1600369600000L, 1605888000000L, "yyyyMMdd");
//    indexs.stream().forEach(i -> System.out.println(i));
//    Map<String, Object> jsonMap = new HashMap<>();
//    jsonMap.put("agent_id", "cba");
//    esClient.insertData("test","test_type","1",jsonMap);
//    esClient.updateData("test","test_type","1",jsonMap);
//    esClient.deleteData("test","test_type","1");
//    esClient.getData("test","test_type","1");
//    esClient.keepAlive();
//    SearchRequest searchRequest = new SearchRequest("test");
//    searchRequest.types("bangcle_type");
//    IndicesOptions options = IndicesOptions.fromOptions(true,true,true,false,false,true,false,false,false);
//    EnumSet<Option> opts = EnumSet.noneOf(IndicesOptions.Option.class);
//    System.out.println(opts);
//    IndicesOptions none_options = new IndicesOptions(opts, WildcardStates.NONE);
//    searchRequest.indicesOptions(none_options);
//    System.out.println(searchRequest.indicesOptions());
//    SearchResponse searchResponse = esClient.getClient().search(searchRequest,RequestOptions.DEFAULT);
//    SearchResponse response = esClient
//        .prepareSearch("bb_i_appinfo_list").execute().actionGet();
//    esClient.prepareSearch("bb_i_appinfo_list").execute().actionGet();
    System.out.println("finish!");
    esClient.closeBulkProcessor();
    esClient.closeClient();
//    esClient.closeBulkProcessor();
    System.exit(0);
  }
}
