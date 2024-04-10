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
package org.apache.pulsar.io.elasticsearch.client.opensearch;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchModule;
import org.opensearch.search.sort.SortBuilder;

@Slf4j
public class OpenSearchUtil {

  private static final NamedXContentRegistry registry = getXContentRegistry();
  public static List<SortBuilder<?>> parseSortJson(String sortJson) throws IOException {
    if (StringUtils.isBlank(sortJson)){
      return Collections.emptyList();
    }
    try (XContentParser parser = getParser(sortJson)) {
      return SortBuilder.fromXContent(parser);
    } catch (IOException e) {
      log.error("Error parsing sort JSON: {}", sortJson, e);
      throw e;
    }
  }

  public static XContentParser getParser(String json) throws IOException {
    XContentParser parser = JsonXContent.jsonXContent.createParser(registry,
            DeprecationHandler.IGNORE_DEPRECATIONS, json);
    if (parser.currentToken() == null) {
      parser.nextToken();
    }
    return parser;
  }

  private static NamedXContentRegistry getXContentRegistry() {
    return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
  }

}
