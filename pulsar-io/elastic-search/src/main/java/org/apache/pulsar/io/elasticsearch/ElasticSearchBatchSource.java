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
package org.apache.pulsar.io.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.BatchSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;


@Connector(
    name = "elastic_search",
    type = IOType.SOURCE,
    help = "A batch source connector that reads elasticsearch documents into a pulsar topic",
    configClass = ElasticSearchConfig.class
)
@Slf4j
public class ElasticSearchBatchSource implements BatchSource<byte[]> {

  private ElasticSearchBatchSourceConfig elasticSearchConfig;
  private ElasticSearchClient elasticsearchClient;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private ObjectMapper sortedObjectMapper;

  @Override
  public void open(Map<String, Object> config, SourceContext context)
      throws Exception {
    elasticSearchConfig = ElasticSearchBatchSourceConfig.load(config, context);
    elasticSearchConfig.validate();
    elasticsearchClient = null; //new ElasticSearchClient(elasticSearchConfig);
    if (elasticSearchConfig.isCanonicalKeyFields()) {
      sortedObjectMapper = JsonMapper
          .builder()
          .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
          .nodeFactory(new JsonNodeFactory() {
            @Override
            public ObjectNode objectNode() {
              return new ObjectNode(this, new TreeMap<String, JsonNode>());
            }

          })
          .build();
    }
  }

  @Override
  public void close() {
    if (elasticsearchClient != null) {
      elasticsearchClient.close();
      elasticsearchClient = null;
    }
  }

  @Override
  public void discover(Consumer<byte[]> taskEater) throws Exception {

  }

  @Override
  public void prepare(byte[] task) throws Exception {

  }

  @Override
  public Record<byte[]> readNext() throws Exception {
    return null;
  }



}
