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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.elasticsearch.ElasticSearchRecord;
import org.apache.pulsar.io.elasticsearch.SlicedSearchTask;

/**
 *
 * @param <R> Search Response type
 * @param <X> Search Hit type
 */
@Slf4j
public abstract class SlicedSearchProvider<R, X> {

    public SlicedSearchProvider() {
    }

    protected final ObjectMapper objectMapper = new ObjectMapper()
            .configure(SerializationFeature.INDENT_OUTPUT, false)
            .setSerializationInclusion(JsonInclude.Include.ALWAYS);

    public abstract void openPit(SlicedSearchTask task) throws IOException;
    public abstract boolean closePit(SlicedSearchTask task) throws IOException;
    public abstract CompletableFuture<? extends R> searchWithPit(SlicedSearchTask task) throws IOException;
    public abstract String getPitIdFromResponse(R response);
    public abstract String getScrollIdFromResponse(R response);
    public abstract Object[] getSortValuesFromLastHit(R response);
    public abstract CompletableFuture<? extends R> startScrollSearch(SlicedSearchTask task) throws IOException;
    public abstract CompletableFuture<? extends R> scrollResults(SlicedSearchTask task) throws IOException;
    public abstract boolean closeScroll(SlicedSearchTask task) throws IOException;
    public abstract List<X> getHits(R response);
    public abstract Map<String, String> buildRecordProperties(X hit);
    public abstract byte[] getHitBytes(X hit);
    public abstract String buildKey(SlicedSearchTask task, X hit, Map<String, String> hitProperties);

    protected ExecutorService executorService = Executors.newWorkStealingPool();
    public ElasticSearchRecord buildRecordFromSearchHit(SlicedSearchTask task, X hit) {
        Map<String, String> properties = Optional.ofNullable(hit)
                .map(this::buildRecordProperties)
                .orElse(Collections.emptyMap());
        byte[] source = Optional.ofNullable(hit).map(this::getHitBytes).orElse(new byte[0]);
        String key = buildKey(task, hit, properties);
        if (StringUtils.isBlank(key)) {
            key = null; //todo if key fields were configured, should this be an error?
        }
        ElasticSearchRecord record = new ElasticSearchRecord(source, key, properties);
        return record;
    }

    public CompletableFuture<Void> slicedScrollSearch(SlicedSearchTask task,
                                                      Consumer<ElasticSearchRecord> recordConsumer)
            throws IOException {
        CompletableFuture<? extends R> searchFut = startScrollSearch(task)
                .thenComposeAsync(response -> handleScrollResponse(task, recordConsumer, response), executorService);
        return searchFut.handleAsync((r, e) -> {
            try {
                boolean closeScrollSuccess = closeScroll(task);
                if (!closeScrollSuccess) {
                    log.warn("Unable to close Scroll: {}", task.getScrollId());
                }
            } catch (IOException ex) {
                log.error("Unable to close Scroll: {}", task.getScrollId());
                if (e != null) {
                    e.addSuppressed(ex);
                } else {
                    throw new CompletionException(ex);
                }
            }
            if (e != null) {
                log.error("Error while searching with scroll", e);
                throw new CompletionException(e);
            }
            return null;
        }, executorService);

    }

    protected CompletableFuture<? extends R> handleScrollResponse(SlicedSearchTask task,
                                                                  Consumer<ElasticSearchRecord> recordConsumer,
                                                                  R previousSearchResponse) {
        if (hasNoResults(previousSearchResponse)) {
            return CompletableFuture.completedFuture(null);
        }
        getHits(previousSearchResponse).stream()
                .filter(Objects::nonNull)
                .map(hit -> this.buildRecordFromSearchHit(task, hit))
                .forEach(recordConsumer);
        updateTaskFromSearchResponse(task, previousSearchResponse);
        try {
            return scrollResults(task).thenComposeAsync(response ->
                            handleScrollResponse(task, recordConsumer, response), executorService);
        } catch (IOException e) {
            throw new CompletionException(e);
        }
    }

    public CompletableFuture<? extends R> handlePitResponse(SlicedSearchTask task,
                                                            Consumer<ElasticSearchRecord> recordConsumer,
                                                            R previousSearchResponse) {
        if (hasNoResults(previousSearchResponse)) {
            return CompletableFuture.completedFuture(null);
        }
        getHits(previousSearchResponse).stream()
                .map(hit -> this.buildRecordFromSearchHit(task, hit))
                .forEach(recordConsumer);
        updateTaskFromSearchResponse(task, previousSearchResponse);
        try {
            return searchWithPit(task).thenComposeAsync(response ->
                            handlePitResponse(task, recordConsumer, response), executorService);
        } catch (IOException e) {
            throw new CompletionException(e);
        }
    }

    public CompletableFuture<Void> slicedPitSearch(SlicedSearchTask task, Consumer<ElasticSearchRecord> recordConsumer)
            throws IOException {
        if (StringUtils.isBlank(task.getPitId())) {
            openPit(task);
        }
        CompletableFuture<? extends R> searchFut = searchWithPit(task)
                .thenComposeAsync(response -> handlePitResponse(task, recordConsumer, response), executorService);
        return searchFut.handleAsync((r, e) -> {
            try {
                boolean pitCloseSuccess = closePit(task);
                if (!pitCloseSuccess) {
                    log.warn("Unable to close PIT: {}", task.getPitId());
                }
            } catch (IOException ex) {
                log.error("Unable to close PIT: {}", task.getPitId());
                if (e != null){
                    e.addSuppressed(ex);
                } else {
                    throw new CompletionException(ex);
                }
            }
            if (e != null) {
                log.error("Error while searching with PIT", e);
                throw new CompletionException(e);
            }
            return null;
        }, executorService);

    }
    public CompletableFuture<Void> slicedSearch(SlicedSearchTask task, Consumer<ElasticSearchRecord> recordConsumer)
            throws IOException {
        return switch (task.getPagingType()) {
            case PIT -> slicedPitSearch(task, recordConsumer);
            case SCROLL -> slicedScrollSearch(task, recordConsumer);
        };
    }

    public boolean hasNoResults(R response) {
        if (response == null) {
            return true;
        }
        List<X> hits = getHits(response);
        return listIsEmpty(hits);
    }

    public void updateTaskFromSearchResponse(SlicedSearchTask task, R response){
        String pitId = getPitIdFromResponse(response);
        if (StringUtils.isNotBlank(pitId)){
            task.setPitId(pitId);
        }
        Object[] sortValues = getSortValuesFromLastHit(response);
        if (sortValues != null && sortValues.length > 0) {
            task.setSearchAfter(sortValues);
        }
        String scrollId = getScrollIdFromResponse(response);
        if (StringUtils.isNotBlank(scrollId)) {
            task.setScrollId(scrollId);
        }
    }

    public boolean listIsEmpty(List<?> list) {
        return list == null || list.isEmpty();
    }
}
