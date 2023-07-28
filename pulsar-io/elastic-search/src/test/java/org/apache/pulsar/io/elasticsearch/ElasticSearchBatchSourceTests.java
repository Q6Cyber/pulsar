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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.elasticsearch.data.UserProfile;
import org.mockito.Mock;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
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
        int bound = Math.max(100000, count * 3);
        Set<UserProfile> userProfiles = new HashSet<>(count);
        while (userProfiles.size() < count) {
            userProfiles.add(buildUserProfile(RND.nextInt(bound)));
        }
        return new ArrayList<>(userProfiles);
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

    @DataProvider(name = "testQuery")
    public Object[][] testQuery() {
        return new Object[][]{
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.SCROLL, 1, 5, 100, 20},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.SCROLL, 1, 100, 100, 20},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.SCROLL, 5, 5, 100, 20},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.SCROLL, 1, 500, 100, 20},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.SCROLL, 5, 500, 100, 20},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.SCROLL, 5, 100, 500, 20},

                new Object[]{ElasticSearchBatchSourceConfig.PagingType.PIT, 1, 5, 100, 20},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.PIT, 1, 100, 100, 20},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.PIT, 5, 5, 100, 20},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.PIT, 1, 500, 100, 20},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.PIT, 5, 500, 100, 20},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.PIT, 5, 100, 500, 20}
        };
    }

    @Test(dataProvider = "discoverSlices")
    public void testDiscoverSlices(ElasticSearchBatchSourceConfig.PagingType pagingType, int numSlices) throws Exception {
        String index = "test" + RND.nextInt(10000);
        map.put("numSlices", numSlices);
        map.put("pagingType", pagingType.toString());
        map.put("indexName", index);

        source.open(map, mockSourceContext);

        ElasticSearchClient client = source.getClient();
        client.getRestClient().createIndex(index);
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

    @Test(dataProvider = "testQuery")
    public void testQuery(ElasticSearchBatchSourceConfig.PagingType pagingType, int numSlices, int pageSize, int count,
                          int searchSize) throws Exception {
        String index = "test" + RND.nextInt(10000);
        List<byte[]> tasks = new ArrayList<>();
        Consumer<byte[]> byteEater = tasks::add;
        map.put("numSlices", numSlices);
        map.put("pageSize", pageSize);
        map.put("indexName", index);
        map.put("pagingType", pagingType.toString());

        List<UserProfile> profiles = getUserProfiles(count);
        Set<UserProfile> profilesToSearchFor = new HashSet<>();
        while (profilesToSearchFor.size() < searchSize) {
            profilesToSearchFor.add(profiles.get(RND.nextInt(count)));
        }

        assertEquals(searchSize, profilesToSearchFor.size());
        QueryBuilder query = QueryBuilders.termsQuery("name", profilesToSearchFor.stream().map(UserProfile::getName).toList());
        map.put("query", query.toString());
        source.open(map, mockSourceContext);
        ElasticSearchClient client = source.getClient();
        setupIndex(index, client, profiles, false);
        source.discover(byteEater);
        assertEquals(tasks.size(), numSlices);
        int totalRecordsRead = 0;
        for (byte[] task : tasks) {
            source.prepare(task);
            Record<ByteBuffer> record;
            source.getCurrentTaskFuture().get(30, TimeUnit.SECONDS);
            Instant start = Instant.now();
            Duration duration = null;
            while ((record = source.readNext()) != null &&
                    (duration = Duration.between(start, Instant.now())).toSeconds() < 30){
                UserProfile profile = objectMapper.readValue(record.getValue().array(), UserProfile.class);
                assertTrue(profilesToSearchFor.contains(profile));
                totalRecordsRead++;
            }
            assertTrue(duration == null || duration.toSeconds() < 30, "Read took longer than 30 seconds");

        }
        assertEquals(totalRecordsRead, searchSize);
    }

    @Test
    public void testSingleKey() throws Exception {
        String index = "test" + RND.nextInt(10000);
        int numSlices = 1;
        List<UserProfile> profiles = getUserProfiles(1);
        map.put("indexName", index);
        map.put("numSlices", numSlices);
        map.put("query", QueryBuilders.idsQuery().addIds(profiles.get(0).getName()).toString());
        map.put("keyFields", "email");
        source.open(map, mockSourceContext);
        ElasticSearchClient client = source.getClient();
        setupIndex(index, client, profiles, true);
        List<byte[]> tasks = new ArrayList<>();
        Consumer<byte[]> byteEater = tasks::add;
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
            assertEquals(records.size(), 1, "Wrong number of records read from index");
            totalRecordsRead += records.size();
            assertEquals(records.get(0).getKey().orElse(""), profiles.get(0).getEmail(),
                    "Wrong key read for record.");
        }
        assertEquals(totalRecordsRead, profiles.size());
    }

    @Test
    public void test2Keys() throws Exception {
        String index = "test" + RND.nextInt(10000);
        int numSlices = 1;
        List<UserProfile> profiles = getUserProfiles(1);
        map.put("indexName", index);
        map.put("numSlices", numSlices);
        map.put("query", QueryBuilders.idsQuery().addIds(profiles.get(0).getName()).toString());
        map.put("keyFields", "email,name");
        source.open(map, mockSourceContext);
        ElasticSearchClient client = source.getClient();
        setupIndex(index, client, profiles, true);
        List<byte[]> tasks = new ArrayList<>();
        Consumer<byte[]> byteEater = tasks::add;
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
            assertEquals(records.size(), 1, "Wrong number of records read from index");
            totalRecordsRead += records.size();
            String expectedHash = DigestUtils.sha256Hex(String.join("", profiles.get(0).getEmail(), profiles.get(0).getName()));
            assertEquals(records.get(0).getKey().orElse(""),expectedHash,"Wrong key read for record.");
        }
        assertEquals(totalRecordsRead, profiles.size());
    }

    private void setupIndex(String index,  ElasticSearchClient client, int count) throws Exception {
        List<UserProfile> userProfiles = getUserProfiles(count);
        setupIndex(index, client, userProfiles, false);
    }

    private void setupIndex(String index, ElasticSearchClient client, List<UserProfile> userProfiles,
                            boolean useNameAsDocId) throws Exception {
        client.getRestClient().createIndex(index);
        userProfiles.forEach(p -> {
            try {
                String json = objectMapper.writeValueAsString(p);
                String id = (useNameAsDocId) ? p.getName() : null;
                boolean success = client.getRestClient().indexDocument(index, id, json);
                assertTrue(success);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        long totalHits = client.getRestClient().totalHits(index);
        assertEquals(userProfiles.size(), totalHits);
    }
}
