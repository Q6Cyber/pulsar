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
package org.apache.pulsar.io.elasticsearch.client.elastic;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SlicedScroll;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.ClearScrollRequest;
import co.elastic.clients.elasticsearch.core.ClearScrollResponse;
import co.elastic.clients.elasticsearch.core.ClosePointInTimeRequest;
import co.elastic.clients.elasticsearch.core.ClosePointInTimeResponse;
import co.elastic.clients.elasticsearch.core.OpenPointInTimeRequest;
import co.elastic.clients.elasticsearch.core.OpenPointInTimeResponse;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.PointInTimeReference;
import co.elastic.clients.elasticsearch.core.search.ResponseBody;
import co.elastic.clients.json.JsonData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.elasticsearch.ElasticSearchBatchSourceConfig;
import org.apache.pulsar.io.elasticsearch.SlicedSearchTask;
import org.apache.pulsar.io.elasticsearch.client.SlicedSearchProvider;

public class ElasticSearchSlicedSearchProvider extends SlicedSearchProvider<ResponseBody<JsonData>, Hit<JsonData>> {
    private final ElasticsearchAsyncClient asyncClient;
    private final ElasticsearchClient client;
    public ElasticSearchSlicedSearchProvider(ElasticsearchClient client, ElasticsearchAsyncClient asyncClient) {
        this.client = client;
        this.asyncClient = asyncClient;
    }

    @Override
    public String getPitIdFromResponse(ResponseBody<JsonData> response) {
        return Optional.ofNullable(response.pitId()).orElse("");
    }

    @Override
    public String getScrollIdFromResponse(ResponseBody<JsonData> response) {
        return Optional.ofNullable(response.scrollId()).orElse("");
    }

    @Override
    public Object[] getSortValuesFromLastHit(ResponseBody<JsonData> response) {
        return Iterables.getLast(response.hits().hits()).sort()
                .stream()
                .map(FieldValue::_get)
                .toArray();
    }

    @Override
    public List<Hit<JsonData>> getHits(ResponseBody<JsonData> response) {
        return response.hits().hits();
    }

    @Override
    public Map<String, String> buildRecordProperties(Hit<JsonData> hit) {
        Map<String, String> properties = new HashMap<>();
        properties.put("id", hit.id());
        properties.put("index", hit.index());
        Optional.ofNullable(hit.shard())
                .ifPresent(shard -> {
                    properties.put("shard", shard);
                });
        properties.put("node", hit.node());
        properties.put("score", String.valueOf(hit.score()));
        properties.put("version", String.valueOf(hit.version()));
        properties.put("seqNo", String.valueOf(hit.seqNo()));
        properties.put("primaryTerm", String.valueOf(hit.primaryTerm()));
        properties.put("sortValues", hit.sort().toString());
        if (hit.matchedQueries() != null && !hit.matchedQueries().isEmpty()) {
            properties.put("matchedQueries", hit.matchedQueries().toString());
        }
        return properties;
    }

    @Override
    public byte[] getHitBytes(Hit<JsonData> hit) {
        return Optional.ofNullable(hit)
                .map(Hit::source)
                .map(JsonData::toString)
                .map(s -> s.getBytes(StandardCharsets.UTF_8))
                .orElse(null);
    }

    @Override
    public String buildKey(Hit hit) {
        //todo
        return null;
    }

    @Override
    public void openPit(SlicedSearchTask task) throws IOException {
        OpenPointInTimeRequest openRequest = new OpenPointInTimeRequest.Builder()
                .index(task.getIndex())
                .keepAlive(new Time.Builder().time(task.getKeepAliveMin() + "m").build())
                .build();
        OpenPointInTimeResponse openResponse = client.openPointInTime(openRequest);
        task.setPitId(openResponse.id());
    }

    @Override
    public boolean closePit(SlicedSearchTask task) throws IOException {
        ClosePointInTimeRequest closeRequest = new ClosePointInTimeRequest.Builder()
                .id(task.getPitId())
                .build();
        ClosePointInTimeResponse closeResponse = client.closePointInTime(closeRequest);
        return closeResponse.succeeded();
    }

    @Override
    public CompletableFuture<SearchResponse<JsonData>> searchWithPit(SlicedSearchTask task) throws IOException {
        SearchRequest.Builder request = buildSearchRequest(task);
        if (StringUtils.isNotBlank(task.getPitId())) {
            PointInTimeReference pitRef = new PointInTimeReference.Builder()
                    .id(task.getPitId())
                    .keepAlive(new Time.Builder().time(task.getKeepAliveMin() + "m").build())
                    .build();
            request.pit(pitRef);
        }
        if (task.hasSearchAfter()) {
            List<FieldValue> fieldValues = Arrays.stream(task.getSearchAfter())
                    .map(this::getFieldValFromObj)
                    .collect(Collectors.toList());
            request.searchAfter(fieldValues);
        }
        return asyncClient.search(request.build(), JsonData.class);
    }

    @Override
    public CompletableFuture<SearchResponse<JsonData>> startScrollSearch(SlicedSearchTask task) throws IOException {
        SearchRequest.Builder request = buildSearchRequest(task);
        request.index(task.getIndex());
        request.scroll(new Time.Builder().time(task.getKeepAliveMin() + "m").build());
        return asyncClient.search(request.build(), JsonData.class);
    }
    @Override
    public CompletableFuture<ScrollResponse<JsonData>> scrollResults(SlicedSearchTask task) throws IOException {
        if (StringUtils.isBlank(task.getScrollId())) {
            CompletableFuture.failedFuture(new IllegalArgumentException("scrollId is null"));
        }
        ScrollRequest scrollRequest = new ScrollRequest.Builder()
                .scrollId(task.getScrollId())
                .scroll(new Time.Builder().time(task.getKeepAliveMin() + "m").build())
                .build();
        return asyncClient.scroll(scrollRequest, JsonData.class);
    }

    @Override
    public boolean closeScroll(SlicedSearchTask task) throws IOException {
        if (StringUtils.isBlank(task.getScrollId())) {
            return true;
        }
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest.Builder()
                .scrollId(task.getScrollId())
                .build();
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest);
        return clearScrollResponse.succeeded();
    }

    private SearchRequest.Builder buildSearchRequest(SlicedSearchTask task) throws IOException {
        SearchRequest.Builder request = new SearchRequest.Builder();
        request.size(task.getSize());
        if (task.getTotalSlices() > 1) {
            SlicedScroll slices = new SlicedScroll.Builder()
                    .max(task.getTotalSlices())
                    .id(Integer.toString(task.getSliceId()))
                    .build();
            request.slice(slices);
        }
        if (StringUtils.isNotBlank(task.getQuery())) {
            Query q = new Query.Builder().withJson(new StringReader(task.getQuery())).build();
            request.query(q);
        }
        if (StringUtils.isNotBlank(task.getSort())) {
            JsonNode sortJsonNode = objectMapper.readTree(task.getSort());
            List<SortOptions> sortOptions = new ArrayList<>();
            if (sortJsonNode.isArray()){
                ArrayNode sortListJson = (ArrayNode) sortJsonNode;
                sortListJson.forEach(sortObj -> {
                    SortOptions sortOption = new SortOptions.Builder()
                            .withJson(new StringReader(sortObj.toString()))
                            .build();
                    sortOptions.add(sortOption);
                });
            } else {
                SortOptions sortOption = new SortOptions.Builder()
                        .withJson(new StringReader(sortJsonNode.toString()))
                        .build();
                sortOptions.add(sortOption);
            }
            request.sort(sortOptions);
        } else {
            getDefaultSort(task.getPagingType()).ifPresent(request::sort);
        }
        return request;
    }

    protected Optional<SortOptions> getDefaultSort(ElasticSearchBatchSourceConfig.PagingType pagingType){
        if (pagingType == ElasticSearchBatchSourceConfig.PagingType.PIT) {
            return Optional.of(SortOptions.of(sb -> sb.field(fo ->
                    fo.field("_shard_doc").order(SortOrder.Asc))));
        } else if (pagingType == ElasticSearchBatchSourceConfig.PagingType.SCROLL) {
            return Optional.of(SortOptions.of(sb -> sb.field(fo -> fo.field("_doc"))));
        }
        return Optional.empty();
    }

    private FieldValue getFieldValFromObj(Object value){
        if (value instanceof Double) {
            return FieldValue.of((Double) value);
        } else if (value instanceof Integer) {
            return FieldValue.of((Integer) value);
        } else if (value instanceof Long) {
            return FieldValue.of((Long) value);
        } else if (value instanceof Boolean) {
            return FieldValue.of((Boolean) value);
        } else {
            return FieldValue.of(value.toString());
        }
    }

}
