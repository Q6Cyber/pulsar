package org.apache.pulsar.io.elasticsearch.opensearch;

import java.io.IOException;
import java.util.List;
import org.apache.pulsar.io.elasticsearch.client.opensearch.OpenSearchUtil;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.testng.annotations.Test;

import static org.opensearch.search.sort.SortOrder.DESC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


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
