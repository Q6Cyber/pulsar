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

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.elasticsearch.data.UserProfile;
import org.mockito.Mock;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public abstract class ElasticSearchBatchSourceTests extends ElasticSearchTestBase {
    private static ElasticsearchContainer container;
    private static final Random RND = new Random();
    protected final ObjectMapper objectMapper = new ObjectMapper();
    @Mock
    protected SourceContext mockSourceContext;
    protected Map<String, Object> map;
    protected ElasticSearchBatchSource source;

    public ElasticSearchBatchSourceTests(String elasticImageName) {
        super(elasticImageName);
    }

    protected List<UserProfile> getUserProfiles(int count) {
        int bound = Math.max(10000, count * 3);
        return IntStream.range(0, count)
                .mapToObj(i -> buildUserProfile(RND.nextInt(bound)))
                .toList();
    }

    protected UserProfile buildUserProfile(int id) {
        return new UserProfile("name" + id, "username" + id, "email" + id + "@domain" + id + ".com",
                null);
    }

    @BeforeMethod(alwaysRun = true)
    public final void initBeforeClass() {
        if (container != null) {
            return;
        }
        container = createElasticsearchContainer();
        container.start();
    }

    @AfterClass(alwaysRun = true)
    public static void closeAfterClass() {
        container.close();
        container = null;
    }

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public final void setUp() throws Exception {

        map = new HashMap<>();
        map.put("elasticSearchUrl", "http://" + container.getHttpHostAddress());
        map.put("schemaEnable", "true");
        map.put("createIndexIfNeeded", "true");
        map.put("bulkConcurrentRequests", 10);
        map.put("bulkEnabled", true);
        map.put("pageSize", 5);


        source = new ElasticSearchBatchSource();
        mockSourceContext = mock(SourceContext.class);

    }

    @AfterMethod(alwaysRun = true)
    public final void tearDown() throws Exception {
        if (source != null) {
            source.close();
        }
    }

    @DataProvider(name = "readSlices")
    public Object[][] readSlices() {
        return new Object[][]{
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.SCROLL, 1, 5, 100},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.SCROLL, 1, 100, 100},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.SCROLL, 5, 5, 100},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.SCROLL, 1, 500, 100},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.SCROLL, 5, 500, 100},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.SCROLL, 5, 100, 500},

                new Object[]{ElasticSearchBatchSourceConfig.PagingType.PIT, 1, 5, 100},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.PIT, 1, 100, 100},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.PIT, 5, 5, 100},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.PIT, 1, 500, 100},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.PIT, 5, 500, 100},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.PIT, 5, 100, 500}
        };
    }

    @DataProvider(name = "discoverSlices")
    public Object[][] discoverSlices() {
        return new Object[][]{
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.SCROLL, 1},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.SCROLL, 5},

                new Object[]{ElasticSearchBatchSourceConfig.PagingType.PIT, 1},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.PIT, 5},
        };
    }

    @Test(dataProvider = "discoverSlices")
    public void testDiscoverSlices(ElasticSearchBatchSourceConfig.PagingType pagingType, int numSlices) throws Exception {
        map.put("numSlices", numSlices);
        map.put("pagingType", pagingType.toString());
        source.open(map, mockSourceContext);
        List<byte[]> tasks = new ArrayList<>();
        Consumer<byte[]> byteEater = tasks::add;
        source.discover(byteEater);
        assertEquals(tasks.size(), numSlices);
        for (int i = 0; i < numSlices; i++) {
            SlicedSearchTask slicedSearchTask = source.deserializeTask(tasks.get(i));
            assertNotNull(slicedSearchTask);
            assertEquals(slicedSearchTask.getSliceId(), i);
            assertEquals(pagingType, slicedSearchTask.getPagingType());
        }
    }

    @Test(dataProvider = "readSlices")
    public void testReadSlices(ElasticSearchBatchSourceConfig.PagingType pagingType, int numSlices, int pageSize, int count) throws Exception {
        String index = "test" + RND.nextInt(10000);
        List<byte[]> tasks = new ArrayList<>();
        Consumer<byte[]> byteEater = tasks::add;
        map.put("numSlices", numSlices);
        map.put("pageSize", pageSize);
        map.put("indexName", index);
        map.put("pagingType", pagingType.toString());

        source.open(map, mockSourceContext);
        ElasticSearchClient client = source.getClient();
        setupIndex(index, client, count);
        source.discover(byteEater);
        assertEquals(tasks.size(), numSlices);
        int totalRecordsRead = 0;
        for (byte[] task : tasks) {
            source.prepare(task);
            Record<ByteBuffer> record;
            List<Record<ByteBuffer>> records = new ArrayList<>();
            source.getCurrentTaskFuture().get(30, TimeUnit.SECONDS);
            Instant start = Instant.now();
            Duration duration = null;
            while ((record = source.readNext()) != null &&
                    (duration = Duration.between(start, Instant.now())).toSeconds() < 30){
                records.add(record);
            }
            assertTrue(duration.toSeconds() < 30, "Read took longer than 30 seconds");
            double evenSliceCount = (double)count / (double) numSlices;
            assertTrue(records.size() > 0, "No records read from slice");
            assertTrue(records.size() <=  count, "Too many records read from slice");
            double percentDif = Math.abs(records.size() - evenSliceCount) / evenSliceCount;
            System.out.println("Slice count: " + records.size() + " even slice count: " + evenSliceCount
                    + " percent dif: " + percentDif);
            totalRecordsRead += records.size();
//            assertTrue(percentDif <= 0.5,
//                    "Number of records from one slice is not within 50% of even distribution. "
//                            + "% dif: " + percentDif + " slice count: " + records.size());
        }
        assertEquals(totalRecordsRead, count);
    }

    private void setupIndex(String index,  ElasticSearchClient client, int count) throws Exception {
        client.getRestClient().createIndex(index);
        List<UserProfile> userProfiles = getUserProfiles(count);
        userProfiles.forEach(p -> {
            try {
                String json = objectMapper.writeValueAsString(p);
                boolean success = client.getRestClient().indexDocument(index, null, json);
                assertTrue(success);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        long totalHits = client.getRestClient().totalHits(index);
        assertEquals(userProfiles.size(), totalHits);
    }
}
