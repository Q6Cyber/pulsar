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
package org.apache.pulsar.io.elasticsearch.opensearch;

import java.util.Arrays;
import org.apache.pulsar.io.elasticsearch.ElasticSearchBatchSourceConfig;
import org.apache.pulsar.io.elasticsearch.ElasticSearchBatchSourceTests;
import org.testng.annotations.DataProvider;

public class OpenSearchBatchSourceTests extends ElasticSearchBatchSourceTests {

    public OpenSearchBatchSourceTests() {
        super(OPENSEARCH);
    }

    @DataProvider(name = "readSlices")
    @Override
    public Object[][] readSlices() {
        //OpenSearch version 1.X does not support PIT
        return Arrays.stream(super.readSlices())
                .filter(optionArr -> optionArr[0].equals(ElasticSearchBatchSourceConfig.PagingType.SCROLL))
                .toArray(Object[][]::new);
    }

    @DataProvider(name = "discoverSlices")
    public Object[][] discoverSlices() {
        return new Object[][]{
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.SCROLL, 1},
                new Object[]{ElasticSearchBatchSourceConfig.PagingType.SCROLL, 5},
        };
    }

    @DataProvider(name = "testQuery")
    public Object[][] testQuery() {
        //OpenSearch version 1.X does not support PIT
        return Arrays.stream(super.testQuery())
                .filter(optionArr -> optionArr[0].equals(ElasticSearchBatchSourceConfig.PagingType.SCROLL))
                .toArray(Object[][]::new);
    }
}
