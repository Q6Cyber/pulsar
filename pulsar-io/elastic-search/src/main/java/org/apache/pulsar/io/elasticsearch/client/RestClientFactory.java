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
package org.apache.pulsar.io.elasticsearch.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.pulsar.io.elasticsearch.ElasticSearchBatchSourceConfig;
import org.apache.pulsar.io.elasticsearch.ElasticSearchConfig;
import org.apache.pulsar.io.elasticsearch.client.elastic.ElasticSearchJavaRestClient;
import org.apache.pulsar.io.elasticsearch.client.opensearch.OpenSearchHighLevelRestClient;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

@Slf4j
public class RestClientFactory {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static RestClient createClient(ElasticSearchConfig config, BulkProcessor.Listener bulkListener)
            throws IOException {
        if (config.getCompatibilityMode() == ElasticSearchConfig.CompatibilityMode.ELASTICSEARCH) {
            log.info("Found compatibilityMode set to '{}', using the ElasticSearch Java client.",
                    config.getCompatibilityMode());
            return new ElasticSearchJavaRestClient(config, bulkListener);
        } else if (config.getCompatibilityMode() == ElasticSearchConfig.CompatibilityMode.ELASTICSEARCH_7
                || config.getCompatibilityMode() == ElasticSearchConfig.CompatibilityMode.OPENSEARCH) {
            log.info("Found compatibilityMode set to '{}', using the OpenSearch High Level Rest API Client.",
                    config.getCompatibilityMode());
            return new OpenSearchHighLevelRestClient(config, bulkListener);
        }
        log.info("Found compatibilityMode set to '{}', will try to auto detect the best client to use.",
                config.getCompatibilityMode());
        try {
            final Map<String, Object> jsonResponse = requestInfo(config);
            ElasticSearchVersion versionInfo = getEsClusterVersion(jsonResponse);
            log.info("Cluster version info: {}", versionInfo);
            if (config instanceof ElasticSearchBatchSourceConfig) {
                validateBatchSourceConfig(versionInfo, (ElasticSearchBatchSourceConfig) config);
            }
            final boolean useOpenSearchHighLevelClient = useOpenSearchHighLevelClient(versionInfo);
            log.info("useOpenSearchHighLevelClient={}, got info response: {}", useOpenSearchHighLevelClient,
                    jsonResponse);
            if (useOpenSearchHighLevelClient) {
                return new OpenSearchHighLevelRestClient(config, bulkListener);
            }
            return new ElasticSearchJavaRestClient(config, bulkListener);
        } catch (IOException ioException) {
            log.warn("Got error while performing info request to detect Elastic version: {}",
                    ioException.getMessage());
            throw ioException;
        }
    }

    private static Map<String, Object> requestInfo(ElasticSearchConfig config) throws IOException {
        try (final OpenSearchHighLevelRestClient openSearchHighLevelRestClient =
                     new OpenSearchHighLevelRestClient(config, null)) {
            final Response response = openSearchHighLevelRestClient.getClient().getLowLevelClient()
                    .performRequest(new Request(HttpGet.METHOD_NAME, "/"));
            return (Map<String, Object>) MAPPER.readValue(response.getEntity().getContent(), Map.class);
        }
    }

    private static ElasticSearchVersion getEsClusterVersion(Map<String, Object> jsonResponse){
        final Map<String, Object> versionMap = (Map<String, Object>) jsonResponse.get("version");
        final String distribution = (String) versionMap.get("distribution");
        final String version = (String) versionMap.getOrDefault("number", "");
        if (!StringUtils.isBlank(distribution)) {
            if (distribution.equals("opensearch")) {
                return ElasticSearchVersion.osFromVersionString(version);
            }
        }
        return ElasticSearchVersion.esFromVersionString(version);
    }

    private static boolean useOpenSearchHighLevelClient(ElasticSearchVersion versionInfo){
        if (ElasticSearchVersion.ClientType.OPENSEARCH.equals(versionInfo.getClientType())) {
            return true;
        }
        //this also covers the case of a bad version string and no distribution (defaults to ElasticSearch)
        if (versionInfo.getMajor() < 7 || (versionInfo.getMajor() == 7 && versionInfo.getMinor() < 14)) {
            return true;
        }
        // For Elastic 7.14+, 8+ use Elastic Java client
        return false;
    }

    private static void validateBatchSourceConfig(ElasticSearchVersion clusterVersion,
                                                  ElasticSearchBatchSourceConfig config){
        switch (config.getPagingType()) {
            case PIT -> validatePitConfig(clusterVersion);
            case SCROLL -> validateScrollConfig(clusterVersion);
        }
    }

    private static void validatePitConfig(ElasticSearchVersion clusterVersion){
        String message = "";
        switch (clusterVersion.getClientType()) {
            case ELASTICSEARCH -> {
                if (clusterVersion.getMajor() <= 7 && clusterVersion.getMinor() < 10) {
                    message = "Point in Time API is not supported in prior to ElasticSearch 7.10.0. "
                            + "Please use Scroll API instead";
                    log.error(message);
                }
            }
            case OPENSEARCH -> {
                if (clusterVersion.getMajor() <= 2 && clusterVersion.getMinor() < 4) {
                    message = "Point in Time API is not supported in prior to OpenSearch 2.4.0. "
                            + "Please use Scroll API instead";
                    log.error(message);
                }
            }
        }
        if (StringUtils.isNotBlank(message)){
            throw new IllegalArgumentException(message);
        }
    }

    private static void validateScrollConfig(ElasticSearchVersion clusterVersion){
        switch (clusterVersion.getClientType()) {
            case ELASTICSEARCH -> {
                if (clusterVersion.getMajor() >= 8) {
                    log.warn("Scroll API is deprecated in ElasticSearch 8.0.0. Please use Point in Time API instead.");
                }
            }
            case OPENSEARCH -> {
            }
        }
    }



}
