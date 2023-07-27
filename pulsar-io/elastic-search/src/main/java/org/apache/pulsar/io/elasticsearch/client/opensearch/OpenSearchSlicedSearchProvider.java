/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.elasticsearch.client.opensearch;

import co.elastic.clients.elasticsearch.core.CreateResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.util.EntityUtils;
import org.apache.pulsar.io.elasticsearch.ElasticSearchBatchSourceConfig;
import org.apache.pulsar.io.elasticsearch.SlicedSearchTask;
import org.apache.pulsar.io.elasticsearch.client.SlicedSearchProvider;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitInfo;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.client.Cancellable;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.Scroll;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.slice.SliceBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;

@Slf4j
public class OpenSearchSlicedSearchProvider extends SlicedSearchProvider<SearchResponse, SearchHit> {

    private RestHighLevelClient client;
    public OpenSearchSlicedSearchProvider(RestHighLevelClient elasticsearchClient) {
        this.client = elasticsearchClient;
    }

    private RestHighLevelClient client() {
        return client;
    }

    @Override
    public Map<String, String> buildRecordProperties(SearchHit hit) {
        Map<String, String> properties = new HashMap<>();
        properties.put("id", hit.getId());
        properties.put("docId", String.valueOf(hit.docId()));
        properties.put("index", hit.getIndex());
        Optional.ofNullable(hit.getShard())
                .ifPresent(shard -> {
                    properties.put("shard", String.valueOf(shard.getShardId()));
                    properties.put("node", String.valueOf(shard.getNodeId()));
                });
        properties.put("score", String.valueOf(hit.getScore()));
        properties.put("version", String.valueOf(hit.getVersion()));
        properties.put("seqNo", String.valueOf(hit.getSeqNo()));
        properties.put("primaryTerm", String.valueOf(hit.getPrimaryTerm()));
        properties.put("sortValues", Arrays.toString(hit.getSortValues()));
        if (hit.getMatchedQueries() != null && hit.getMatchedQueries().length > 0) {
            properties.put("matchedQueries", Arrays.toString(hit.getMatchedQueries()));
        }
        return properties;
    }
    @Override
    public byte[] getHitBytes(SearchHit hit) {
        return Optional.ofNullable(hit.getSourceRef())
                .map(BytesReference::toBytesRef)
                .map(br -> br.bytes)
                .orElse(null);
    }

    @Override
    public String buildKey(SearchHit hit) {
        //todo build key based on configured fields
        return null;
    }

    @Override
    public Object[] getSortValuesFromLastHit(SearchResponse response){
        SearchHit lastHit = response.getHits().getHits()[response.getHits().getHits().length - 1];
        return lastHit.getSortValues();
    }

    @Override
    public String getPitIdFromResponse(SearchResponse response) {
        return Optional.ofNullable(response.pointInTimeId()).orElse("");
    }

    @Override
    public String getScrollIdFromResponse(SearchResponse response) {
        return Optional.ofNullable(response.getScrollId()).orElse("");
    }

    @Override
    public void openPit(SlicedSearchTask task) throws IOException {
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueMinutes(task.getKeepAliveMin()),
                false,
                task.getIndex());
        CreatePitResponse response = client.createPit(request, RequestOptions.DEFAULT);
        if (StringUtils.isNotBlank(response.getId())) {
            task.setPitId(response.getId());
        } else {
            throw new IOException("Unable to open pit: " + response.status());
        }
    }

    @Override
    public boolean closePit(SlicedSearchTask task) throws IOException {
        DeletePitRequest request = new DeletePitRequest(task.getPitId());
        DeletePitResponse response = client.deletePit(request, RequestOptions.DEFAULT);
        boolean success = response.getDeletePitResults().stream().allMatch(DeletePitInfo::isSuccessful);
        if (success) {
            return true;
        } else {
            log.error("Unable to close pit. Status: {} Delete PIT Results: {}",
                    response.status(), response.getDeletePitResults());
            throw new IOException("Unable to close pit. Status: " + response.status());
        }
    }

    @Override
    public CompletableFuture<SearchResponse> searchWithPit(SlicedSearchTask task) throws IOException {
        SearchSourceBuilder searchSourceBuilder = buildSearchSource(task);
        if (StringUtils.isNotBlank(task.getPitId())){
            PointInTimeBuilder pitBuilder = new PointInTimeBuilder(task.getPitId())
                    .setKeepAlive(TimeValue.timeValueMinutes(task.getKeepAliveMin()));
            searchSourceBuilder.pointInTimeBuilder(pitBuilder);
        }
        if (task.hasSearchAfter()) {
            searchSourceBuilder.searchAfter(task.getSearchAfter());
        }

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);
        CompletableFuture<SearchResponse> responseFuture = new CompletableFuture<>();
        client.searchAsync(searchRequest, RequestOptions.DEFAULT, buildActionListener(responseFuture));
        return responseFuture;
    }

    @Override
    public CompletableFuture<SearchResponse> startScrollSearch(SlicedSearchTask task) throws IOException {
        SearchSourceBuilder searchSourceBuilder = buildSearchSource(task);
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(task.getKeepAliveMin()));
        final CompletableFuture<SearchResponse> responseFuture = new CompletableFuture<>();
        SearchRequest searchRequest = new SearchRequest(task.getIndex());
        searchRequest.scroll(scroll);
        searchRequest.source(searchSourceBuilder);
        client.searchAsync(searchRequest, RequestOptions.DEFAULT, buildActionListener(responseFuture));
        return responseFuture;
    }

    @Override
    public CompletableFuture<SearchResponse> scrollResults(SlicedSearchTask task) throws IOException {
        if (StringUtils.isBlank(task.getScrollId())) {
            CompletableFuture.failedFuture(new IllegalArgumentException("scrollId is null"));
        }
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(task.getKeepAliveMin()));
        final CompletableFuture<SearchResponse> responseFuture = new CompletableFuture<>();
        ActionListener<SearchResponse> actionListener = buildActionListener(responseFuture);
        SearchScrollRequest scrollRequest = new SearchScrollRequest(task.getScrollId());
        scrollRequest.scroll(scroll);
        Cancellable cancellable = client.searchScrollAsync(scrollRequest, RequestOptions.DEFAULT, actionListener);
        return responseFuture;
    }

    @Override
    public boolean closeScroll(SlicedSearchTask task) throws IOException {
        if (StringUtils.isBlank(task.getScrollId())) {
            return true;
        }
        ClearScrollRequest request = new ClearScrollRequest();
        request.addScrollId(task.getScrollId());
        ClearScrollResponse response = client.clearScroll(request, RequestOptions.DEFAULT);
        return response.isSucceeded();
    }
    @Override
    public List<SearchHit> getHits(SearchResponse response) {
        return Optional.ofNullable(response)
                .map(SearchResponse::getHits)
                .map(SearchHits::getHits)
                .stream()
                .flatMap(Arrays::stream)
                .collect(Collectors.toList());
    }

    public SearchSourceBuilder buildSearchSource(SlicedSearchTask task) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        if (StringUtils.isNotBlank(task.getQuery())) {
            searchSourceBuilder.query(QueryBuilders.wrapperQuery(task.getQuery()));
        }
        searchSourceBuilder.size(task.getSize());
        if (task.getTotalSlices() > 1){
            searchSourceBuilder.slice(new SliceBuilder(task.getSliceId(), task.getTotalSlices()));
        }
        if (StringUtils.isNotBlank(task.getSort())) {
            List<SortBuilder<?>> sortBuilders = OpenSearchUtil.parseSortJson(task.getSort());
            if (sortBuilders != null && !sortBuilders.isEmpty()){
                sortBuilders.forEach(searchSourceBuilder::sort);
            }
        } else {
            getDefaultSort(task.getPagingType()).ifPresent(searchSourceBuilder::sort);
        }
        return searchSourceBuilder;
    }

    protected Optional<SortBuilder<?>> getDefaultSort(ElasticSearchBatchSourceConfig.PagingType pagingType){
        if (pagingType == ElasticSearchBatchSourceConfig.PagingType.PIT){
            return Optional.of(SortBuilders.fieldSort("_doc"));
        } else if (pagingType == ElasticSearchBatchSourceConfig.PagingType.SCROLL) {
            return Optional.of(SortBuilders.fieldSort("_doc"));
        }
        return Optional.empty();
    }

    public <X> ActionListener<X> buildActionListener(CompletableFuture<X> responseFuture){
        return new ActionListener<>() {
            @Override
            public void onResponse(X response) {
                responseFuture.complete(response);
            }

            @Override
            public void onFailure(Exception e) {
                responseFuture.completeExceptionally(e);
            }
        };
    }

}
