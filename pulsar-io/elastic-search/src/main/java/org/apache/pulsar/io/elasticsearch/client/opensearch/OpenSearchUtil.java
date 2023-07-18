package org.apache.pulsar.io.elasticsearch.client.opensearch;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.ContextParser;
import org.opensearch.common.xcontent.DeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.sort.SortBuilder;

@Slf4j
public class OpenSearchUtil {

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
    XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.IGNORE_DEPRECATIONS, json);
    if (parser.currentToken() == null) {
      parser.nextToken();
    }

    return parser;
  }

}
