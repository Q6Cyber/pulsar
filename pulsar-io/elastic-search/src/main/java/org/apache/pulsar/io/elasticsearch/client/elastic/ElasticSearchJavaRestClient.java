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
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch._types.SlicedScroll;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.ClearScrollRequest;
import co.elastic.clients.elasticsearch.core.ClearScrollResponse;
import co.elastic.clients.elasticsearch.core.ClosePointInTimeRequest;
import co.elastic.clients.elasticsearch.core.ClosePointInTimeResponse;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.OpenPointInTimeRequest;
import co.elastic.clients.elasticsearch.core.OpenPointInTimeResponse;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.PointInTimeReference;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.pulsar.io.elasticsearch.ElasticSearchConfig;
import org.apache.pulsar.io.elasticsearch.client.BulkProcessor;
import org.apache.pulsar.io.elasticsearch.client.RestClient;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClientBuilder;

@Slf4j
public class ElasticSearchJavaRestClient extends RestClient {

    private final ElasticsearchClient client;

    private final ElasticsearchAsyncClient asyncClient;
    private BulkProcessor bulkProcessor;
    private ElasticsearchTransport transport;

    @VisibleForTesting
    public void setBulkProcessor(BulkProcessor bulkProcessor) {
        this.bulkProcessor = bulkProcessor;
    }

    @VisibleForTesting
    public void setTransport(ElasticsearchTransport transport) {
        this.transport = transport;
    }

    public ElasticSearchJavaRestClient(ElasticSearchConfig elasticSearchConfig,
                                       BulkProcessor.Listener bulkProcessorListener) {
        super(elasticSearchConfig, bulkProcessorListener);

        log.info("ElasticSearch URL {}", config.getElasticSearchUrl());
        final HttpHost[] httpHosts = getHttpHosts();

        RestClientBuilder builder = org.elasticsearch.client.RestClient.builder(httpHosts)
                .setRequestConfigCallback(builder1 -> builder1
                        .setContentCompressionEnabled(config.isCompressionEnabled())
                        .setConnectionRequestTimeout(config.getConnectionRequestTimeoutInMs())
                        .setConnectTimeout(config.getConnectTimeoutInMs())
                        .setSocketTimeout(config.getSocketTimeoutInMs()))
                .setHttpClientConfigCallback(this.configCallback)
                .setFailureListener(new org.elasticsearch.client.RestClient.FailureListener() {
                    public void onFailure(Node node) {
                        log.warn("Node host={} failed", node.getHost());
                    }
                });
        transport = new RestClientTransport(builder.build(), new JacksonJsonpMapper(objectMapper));
        client = new ElasticsearchClient(transport);
        asyncClient = new ElasticsearchAsyncClient(transport);
        if (elasticSearchConfig.isBulkEnabled()) {
            bulkProcessor = new ElasticBulkProcessor(elasticSearchConfig, client, bulkProcessorListener);
        } else {
            bulkProcessor = null;
        }
    }

    @Override
    public boolean indexExists(String index) throws IOException {
        final ExistsRequest request = new ExistsRequest.Builder()
                .index(index)
                .build();
        return client.indices().exists(request).value();
    }

    @Override
    public boolean createIndex(String index) throws IOException {
        final CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder()
                .index(index)
                .settings(new IndexSettings.Builder()
                        .numberOfShards(config.getIndexNumberOfShards() + "")
                        .numberOfReplicas(config.getIndexNumberOfReplicas() + "")
                        .build()
                )
                .build();
        try {
            final CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest);
            if ((createIndexResponse.acknowledged())
                    && createIndexResponse.shardsAcknowledged()) {
                return true;
            }
            throw new IOException("Unable to create index, acknowledged: " + createIndexResponse.acknowledged()
                    + " shardsAcknowledged: " + createIndexResponse.shardsAcknowledged());
        } catch (ElasticsearchException ex) {
            final String errorType = Objects.requireNonNull(ex.response().error().type());
            if (errorType.contains("resource_already_exists_exception")) {
                return false;
            }
            throw ex;
        }
    }

    @Override
    public boolean deleteIndex(String index) throws IOException {
        return client.indices().delete(new DeleteIndexRequest.Builder().index(index).build()).acknowledged();
    }

    @Override
    public boolean deleteDocument(String index, String documentId) throws IOException {
        final DeleteRequest req = new
                DeleteRequest.Builder()
                .index(config.getIndexName())
                .id(documentId)
                .build();

        DeleteResponse deleteResponse = client.delete(req);
        return deleteResponse.result().equals(Result.Deleted) || deleteResponse.result().equals(Result.NotFound);
    }

    @Override
    public boolean indexDocument(String index, String documentId, String documentSource) throws IOException {
        final Map mapped = objectMapper.readValue(documentSource, Map.class);
        final IndexRequest<Object> indexRequest = new IndexRequest.Builder<>()
                .index(config.getIndexName())
                .document(mapped)
                .id(documentId)
                .build();
        final IndexResponse indexResponse = client.index(indexRequest);

        return indexResponse.result().equals(Result.Created) || indexResponse.result().equals(Result.Updated);
    }

    public SearchResponse<Map> search(String indexName) throws IOException {
        return search(indexName, "*:*");
    }

    @VisibleForTesting
    public SearchResponse<Map> search(String indexName, String query) throws IOException {
        final RefreshRequest refreshRequest = new RefreshRequest.Builder().index(indexName).build();
        client.indices().refresh(refreshRequest);

        query = query.replace("/", "\\/");
        return client.search(new SearchRequest.Builder().index(indexName)
                .q(query)
                .build(), Map.class);
    }

    @Override
    public long totalHits(String indexName) throws IOException {
        return totalHits(indexName, "*:*");
    }

    @Override
    public long totalHits(String indexName, String query) throws IOException {
        final SearchResponse<Map> searchResponse = search(indexName, query);
        return searchResponse.hits().total().value();
    }

    public String openPit(String index, int keepAliveMin) throws IOException {
        OpenPointInTimeRequest openRequest = new OpenPointInTimeRequest.Builder()
            .index(index)
            .keepAlive(new Time.Builder().time(keepAliveMin + "m").build())
            .build();
        OpenPointInTimeResponse openResponse = client.openPointInTime(openRequest);
        return openResponse.id();
    }

    public boolean closePit(String pit) throws IOException {
        ClosePointInTimeRequest closeRequest = new ClosePointInTimeRequest.Builder()
            .id(pit)
            .build();
        ClosePointInTimeResponse closeResponse = client.closePointInTime(closeRequest);
        return closeResponse.succeeded();
    }

    public CompletableFuture<SearchResponse<Map>> searchWithPit(String pit, int keepAliveMin, String query,
                                                                Object[] searchAfterArr, String sort, int size,
                                                                int maxSlices, int sliceId) throws IOException {
        SearchRequest.Builder request = buildSearchRequest(query, sort, size, maxSlices, sliceId);
        if (StringUtils.isNotBlank(pit)) {
            PointInTimeReference pitRef = new PointInTimeReference.Builder()
                .id(pit)
                .keepAlive(new Time.Builder().time(keepAliveMin + "m").build())
                .build();
            request.pit(pitRef);
        }
        if (searchAfterArr != null && searchAfterArr.length > 0) {
            List<FieldValue> fieldValues = Arrays.stream(searchAfterArr)
                .map(this::getFieldValFromObj)
                .collect(Collectors.toList());
            request.searchAfter(fieldValues);
        }
        return asyncClient.search(request.build(), Map.class);
    }

    public CompletableFuture<SearchResponse<Map>> startScrollSearch(int keepAliveMin, String query,
                                                                    String sort, int size, int maxSlices, int sliceId)
            throws IOException {
        SearchRequest.Builder request = buildSearchRequest(query, sort, size, maxSlices, sliceId);
        request.scroll(new Time.Builder().time(keepAliveMin + "m").build());
        return asyncClient.search(request.build(), Map.class);
    }

    public CompletableFuture<ScrollResponse<Map>> scrollResults(String scrollId, int keepAliveMin) throws IOException {
        ScrollRequest scrollRequest = new ScrollRequest.Builder()
                .scrollId(scrollId)
                .scroll(new Time.Builder().time(keepAliveMin + "m").build())
                .build();
        return asyncClient.scroll(scrollRequest, Map.class);
    }

    public boolean closeScroll(String scrollId) throws IOException {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest.Builder()
                .scrollId(scrollId)
                .build();
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest);
        return clearScrollResponse.succeeded();
    }

    public SearchRequest.Builder buildSearchRequest(String query, String sort, int size, int maxSlices, int sliceId)
            throws IOException {
        SearchRequest.Builder request = new SearchRequest.Builder();
        request.size(size);
        if (maxSlices > 1) {
            SlicedScroll slices = new SlicedScroll.Builder()
                    .max(maxSlices)
                    .id(Integer.toString(sliceId))
                    .build();
            request.slice(slices);
        }
        if (StringUtils.isNotBlank(query)) {
            Query q = new Query.Builder().withJson(new StringReader(query)).build();
            request.query(q);
        }
        if (StringUtils.isNotBlank(sort)) {
            JsonNode sortJsonNode = objectMapper.readTree(sort);
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
        }
        return request;
    }


    @Override
    public BulkProcessor getBulkProcessor() {
        if (bulkProcessor == null) {
            throw new IllegalStateException("bulkProcessor not enabled");
        }
        return bulkProcessor;
    }

    @Override
    public void closeClient() {
        if (bulkProcessor != null) {
            bulkProcessor.close();
        }
        // client doesn't need to be closed, only the transport instance
        try {
            transport.close();
        } catch (IOException e) {
            log.warn("error while closing the client", e);
        }
    }

    @VisibleForTesting
    public ElasticsearchClient getClient() {
        return client;
    }

    @VisibleForTesting
    public ElasticsearchTransport getTransport() {
        return transport;
    }

    private FieldValue getFieldValue(JsonNode val) {
        if (val.isValueNode()){
            ValueNode valueNode = (ValueNode) val;
            if (valueNode.canConvertToLong()){
                return FieldValue.of(valueNode.asLong());
            } else if (valueNode.isFloatingPointNumber()){
                return FieldValue.of(valueNode.asDouble());
            } else if (valueNode.isTextual()) {
                return FieldValue.of(valueNode.asText());
            } else if (valueNode.isBoolean()) {
                return FieldValue.of(valueNode.asBoolean());
            }
        }
        return null;
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
