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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.common.IOConfigUtils;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;


/**
 * Configuration class for the ElasticSearch BatchSource Connector.
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Accessors(chain = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ElasticSearchBatchSourceConfig extends ElasticSearchConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
        required = false,
        defaultValue = "SCROLL",
        help = "The type of paging to use, Point in Time or Scroll."
    )
    private PagingType pagingType = PagingType.SCROLL;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "The comma separated ordered list of field names used to build the Pulsar key from the record value. "
            + "The ElasticSearch Hit properties are also available, this will include the ElasticSearch ID, index, and "
            + "other metadata."
            + "If this list is a singleton, the field is converted as a string. If this list has 2 or more fields, "
            + "the generated key is a SHA256 of the concatenated values of the list."
    )
    private String keyFields = "";

    @FieldDoc(
        required = false,
        defaultValue = "1",
        help = "The number of slices to split the query into. Each slice will be loaded in parallel."
    )
    private int numSlices = 1;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "The JSON of the query to execute. For example: { \"match_all\": {} } "
    )
    private String query = "";

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "The JSON of the sorting to perform on the query. For example: [ \n"
            + "    {\"@timestamp\": {\"order\": \"asc\", \"format\": \"strict_date_optional_time_nanos\"}},\n"
            + "    {\"_shard_doc\": \"desc\"}\n" + "  ] "
    )
    private String sort = "";

    @FieldDoc(
        required = false,
        defaultValue = "1000",
        help = "The number of results to return in each sliced search batch."
    )
    private int pageSize = 1000;

    @FieldDoc(
            required = false,
            defaultValue = "2",
            help = "The number of minutes to keep a Scroll or PIT alive. "
                    + "This is only the time required to consume one page of results, not all results."
    )
    private int keepAliveMin = 2;
    public enum PagingType {
        PIT,
        SCROLL
    }

    public static ElasticSearchBatchSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), ElasticSearchBatchSourceConfig.class);
    }

    public static ElasticSearchBatchSourceConfig load(Map<String, Object> map, SourceContext sourceContext)
        throws IOException {
        return IOConfigUtils.loadWithSecrets(map, ElasticSearchBatchSourceConfig.class, sourceContext);
    }
    public void validate() {
        super.validate();

        if (numSlices < 1) {
            throw new IllegalArgumentException("numSlices must be greater than 0");
        }
        if (pageSize < 1) {
            throw new IllegalArgumentException("pageSize must be greater than 0");
        }
        if (pageSize > 10_000) {
            throw new IllegalArgumentException("pageSize must be less than 10,000");
        }
    }

    public List<String> getKeyFieldsList() {
        if (StringUtils.isBlank(keyFields)) {
            return Collections.emptyList();
        }
        return Arrays.asList(keyFields.split(","));
    }
}
