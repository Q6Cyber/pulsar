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

import static org.opensearch.search.sort.SortOrder.DESC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.io.IOException;
import java.util.List;
import org.apache.pulsar.io.elasticsearch.client.opensearch.OpenSearchUtil;
import org.opensearch.search.sort.SortBuilder;
import org.testng.annotations.Test;


public class OpensearchXContentTests {

  @Test
  public void testParseSortJson() throws IOException {
    String sortJson = "[{\"grades\" : {\"order\" : \"desc\", \"mode\" : \"avg\"}}]";
    List<SortBuilder<?>> sortBuilders = OpenSearchUtil.parseSortJson(sortJson);
    assertEquals(1, sortBuilders.size());
    SortBuilder<?> sortBuilder = sortBuilders.get(0);
    assertNotNull(sortBuilder);
    assertEquals(DESC, sortBuilder.order());
  }
}
