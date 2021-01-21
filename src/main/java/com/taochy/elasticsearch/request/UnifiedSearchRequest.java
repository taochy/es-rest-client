package com.taochy.elasticsearch.request;

import com.taochy.elasticsearch.client.EsClient;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

/**
 * @author ：taochy
 * @date ：Created in 2020/12/14 11:22 上午
 * @description：扩展SearchRequest，留待后续功能扩展
 * @modified By：
 * @version: 1.0.0.0
 */
@Slf4j
public class UnifiedSearchRequest {

    private RestHighLevelClient rhlClient;
    private SearchRequest searchRequest;
    private SearchResponse response;

    public UnifiedSearchRequest(EsClient client, String... indices) {
        searchRequest = new SearchRequest(indices);
        this.rhlClient = client.getClient();
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
    }

    public UnifiedSearchRequest setTypes(String... types) {
        searchRequest.types(types);
        return this;
    }

    public UnifiedSearchRequest setQuery(QueryBuilder queryBuilder) {
        searchRequest.source().query(queryBuilder);
        return this;
    }

    public UnifiedSearchRequest addAggregation(AggregationBuilder aggregation) {
        searchRequest.source().aggregation(aggregation);
        return this;
    }

    public UnifiedSearchRequest setFrom(int from) {
        searchRequest.source().from(from);
        return this;
    }

    public UnifiedSearchRequest setSize(int size) {
        searchRequest.source().size(size);
        return this;
    }

    public UnifiedSearchRequest addSort(String sortCol, SortOrder order) {
        searchRequest.source().sort(sortCol, order);
        return this;
    }

    public UnifiedSearchRequest addSort(SortBuilder<?> builder) {
        searchRequest.source().sort(builder);
        return this;
    }

    public UnifiedSearchRequest setFetchSource(boolean fetch) {
        searchRequest.source().fetchSource(fetch);
        return this;
    }

    public UnifiedSearchRequest setFetchSource(@Nullable String include, @Nullable String exclude) {
        searchRequest.source().fetchSource(include, exclude);
        return this;
    }

    public UnifiedSearchRequest setFetchSource(@Nullable String[] includes,
                                               @Nullable String[] excludes) {
        searchRequest.source().fetchSource(includes, excludes);
        return this;
    }

    public UnifiedSearchRequest setScroll(TimeValue keepAlive) {
        searchRequest.scroll(keepAlive);
        return this;
    }

    public UnifiedSearchRequest setCollapse(CollapseBuilder collapseBuilder) {
        searchRequest.source().collapse(collapseBuilder);
        return this;
    }

    public UnifiedSearchRequest setSearchType(SearchType searchType) {
        searchRequest.searchType(searchType);
        return this;
    }

    public UnifiedSearchRequest execute() {
        try {
            System.out.println(searchRequest.indicesOptions().toString());
            response = rhlClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error(e.getMessage());
        } finally {
            return this;
        }
    }

    public SearchResponse actionGet() {
        return response;
    }

}
