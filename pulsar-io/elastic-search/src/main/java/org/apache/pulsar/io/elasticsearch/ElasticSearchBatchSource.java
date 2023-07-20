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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.core.BatchPushSource;
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
public class ElasticSearchBatchSource extends BatchPushSource<ByteBuffer> {

  private ElasticSearchBatchSourceConfig elasticSearchConfig;
  private ElasticSearchClient elasticsearchClient;
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public int getQueueLength() {
    return 20_000; //set for 2 x max batch size.
    // Cannot be set based on config because it is called from parent constructor before open is called.
  }

  @Override
  public void open(Map<String, Object> config, SourceContext context)
      throws Exception {
    elasticSearchConfig = ElasticSearchBatchSourceConfig.load(config, context);
    elasticSearchConfig.validate();
    elasticsearchClient = new ElasticSearchClient(elasticSearchConfig);
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
    int numSlices = elasticSearchConfig.getNumSlices();
    for (int i = 0; i < numSlices; i++) {
      SlicedSearchTask task =
              switch (elasticSearchConfig.getPagingType()) {
                case SCROLL -> buildScrollTask(i);
                case PIT -> builtPitTask(i);
              };
      taskEater.accept(objectMapper.writeValueAsBytes(task));
    }
  }

  public SlicedSearchTask buildScrollTask(int sliceId){
    return SlicedSearchTask.buildFirstScrollTask(
            elasticSearchConfig.getIndexName(),
            elasticSearchConfig.getQuery(),
            elasticSearchConfig.getSort(),
            elasticSearchConfig.getPageSize(),
            elasticSearchConfig.getKeepAliveMin(),
            sliceId,
            elasticSearchConfig.getNumSlices());
  }

  public SlicedSearchTask builtPitTask(int sliceId){
    return SlicedSearchTask.buildFirstPitTask(
            elasticSearchConfig.getIndexName(),
            elasticSearchConfig.getQuery(),
            elasticSearchConfig.getSort(),
            elasticSearchConfig.getPageSize(),
            elasticSearchConfig.getKeepAliveMin(),
            sliceId,
            elasticSearchConfig.getNumSlices());
  }

  @Override
  public void prepare(byte[] task) throws Exception {
    SlicedSearchTask slicedSearchTask = objectMapper.readValue(task, SlicedSearchTask.class);
    log.debug("Executing task {}", slicedSearchTask);
    Consumer<ElasticSearchRecord> recordConsumer = this::consume;
    CompletableFuture<Void> taskFut = elasticsearchClient.getSlicedSearchProvider()
            .slicedSearch(slicedSearchTask, recordConsumer);
    taskFut.whenComplete((aVoid, throwable) -> {
      this.consume(null);
      if (throwable != null){
        log.error("Error while executing task {}", slicedSearchTask, throwable);
      } else {
        log.debug("Task {} completed", slicedSearchTask);
      }
    });
  }
}
