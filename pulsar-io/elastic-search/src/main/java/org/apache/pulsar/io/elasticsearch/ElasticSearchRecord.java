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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Record;

public class ElasticSearchRecord implements Record<ByteBuffer> {
    private final ByteBuffer byteBuffer;
    private final String key;
    private final Map<String, String> properties;

    public ElasticSearchRecord(byte[] sourceBytes, String key, Map<String, String> properties) {
        this.byteBuffer = ByteBuffer.wrap(sourceBytes);
        this.key = key;
        this.properties = properties;
    }

    @Override
    public ByteBuffer getValue() {
        return byteBuffer;
    }

    @Override
    public Optional<String> getKey() {
        return Optional.ofNullable(key);
    }

    @Override
    public Schema<ByteBuffer> getSchema() {
        return Schema.BYTEBUFFER;
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }
}
