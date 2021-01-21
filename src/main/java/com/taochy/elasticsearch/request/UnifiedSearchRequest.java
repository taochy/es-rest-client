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
public class UnifiedSearchRequest extends SearchRequest {

    private RestHighLevelClient rhlClient;
    private SearchResponse response;

    public UnifiedSearchRequest(EsClient client, String... indices) {
        super(indices);
        this.rhlClient = client.getClient();
        this.indicesOptions(IndicesOptions.lenientExpandOpen());
    }

    public UnifiedSearchRequest setQuery(QueryBuilder queryBuilder) {
        this.source().query(queryBuilder);
        return this;
    }

    public UnifiedSearchRequest addAggregation(AggregationBuilder aggregation) {
        this.source().aggregation(aggregation);
        return this;
    }

    public UnifiedSearchRequest setFrom(int from) {
        this.source().from(from);
        return this;
    }

    public UnifiedSearchRequest setSize(int size) {
        this.source().size(size);
        return this;
    }

    public UnifiedSearchRequest addSort(String sortCol, SortOrder order) {
        this.source().sort(sortCol, order);
        return this;
    }

    public UnifiedSearchRequest addSort(SortBuilder<?> builder) {
        this.source().sort(builder);
        return this;
    }

    public UnifiedSearchRequest setFetchSource(boolean fetch) {
        this.source().fetchSource(fetch);
        return this;
    }

    public UnifiedSearchRequest setFetchSource(@Nullable String include, @Nullable String exclude) {
        this.source().fetchSource(include, exclude);
        return this;
    }

    public UnifiedSearchRequest setFetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        this.source().fetchSource(includes, excludes);
        return this;
    }

    public UnifiedSearchRequest setScroll(TimeValue keepAlive) {
        this.scroll(keepAlive);
        return this;
    }

    public UnifiedSearchRequest setCollapse(CollapseBuilder collapseBuilder) {
        this.source().collapse(collapseBuilder);
        return this;
    }

    public UnifiedSearchRequest setSearchType(SearchType searchType) {
        this.searchType(searchType);
        return this;
    }

    public UnifiedSearchRequest execute() {
        try {
            response = rhlClient.search(this, RequestOptions.DEFAULT);
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
