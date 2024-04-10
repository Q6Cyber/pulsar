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

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
@ToString
public class ElasticSearchVersion {
    public enum ClientType {
        ELASTICSEARCH,
        OPENSEARCH
    }

    private int major = 0;
    private int minor = 0;
    private int patch = 0;
    private ClientType clientType;

    private ElasticSearchVersion(ClientType clientType, String versionString) {
        String[] versionParts = versionString.split("\\.");
        try {
            if (versionParts.length > 0) {
                major = Integer.parseInt(versionParts[0]);
            }
            if (versionParts.length > 1) {
                minor = Integer.parseInt(versionParts[1]);
            }
            if (versionParts.length > 2) {
                patch = Integer.parseInt(versionParts[2]);
            }
        } catch (NumberFormatException nfe) {
            log.warn("Not able to parse version: {}", versionString, nfe);
        }
        this.clientType = clientType;
    }

    public static ElasticSearchVersion esFromVersionString(String version){
        return fromVersionString(ClientType.ELASTICSEARCH, version);
    }

    public static ElasticSearchVersion osFromVersionString(String version){
        return fromVersionString(ClientType.OPENSEARCH, version);
    }

    public static ElasticSearchVersion fromVersionString(ClientType clientType, String version){
        return new ElasticSearchVersion(clientType, version);
    }
}


