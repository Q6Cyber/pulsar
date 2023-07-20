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

import java.io.Serializable;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.Accessors;


@Data
@Accessors(chain = true)
@EqualsAndHashCode
@ToString
@Builder
public class SlicedSearchTask implements Serializable {
  private String index;
  private String query;
  private String sort;
  private ElasticSearchBatchSourceConfig.PagingType pagingType;
  private int size;
  private int sliceId;
  private int totalSlices;
  private String pitId;
  private String scrollId;
  private int keepAliveMin;
  private Object[] searchAfter;

  public static SlicedSearchTask buildFirstScrollTask(String index, String query, String sort, int size,
                                                      int keepAliveMin, int sliceId, int totalSlices) {
    return SlicedSearchTask.builder()
            .pagingType(ElasticSearchBatchSourceConfig.PagingType.SCROLL)
            .index(index)
            .query(query)
            .sort(sort)
            .size(size)
            .keepAliveMin(keepAliveMin)
            .sliceId(sliceId)
            .totalSlices(totalSlices)
            .build();
  }

  public static SlicedSearchTask buildFirstPitTask(String index, String query, String sort, int size, int keepAliveMin,
                                            int sliceId, int totalSlices){
    return SlicedSearchTask.builder()
            .pagingType(ElasticSearchBatchSourceConfig.PagingType.PIT)
            .index(index)
            .query(query)
            .sort(sort)
            .size(size)
            .keepAliveMin(keepAliveMin)
            .sliceId(sliceId)
            .totalSlices(totalSlices)
            .build();
  }

  public boolean hasSearchAfter(){
    return this.searchAfter != null && this.searchAfter.length > 0;
  }

//  public SlicedSearchTask updatePitTask(String pitId){
//    if (this.pagingType != ElasticSearchBatchSourceConfig.PagingType.PIT){
//      throw new IllegalStateException("This is not a PIT task");
//    }
//    setPitId(pitId);
//    return this;
//  }
//
//  public SlicedSearchTask updateSearchAfter(Object[] searchAfter){
//    setSearchAfter(searchAfter);
//    return this;
//  }
//
//  public SlicedSearchTask updateScrollTask(String scrollId){
//    if (this.pagingType != ElasticSearchBatchSourceConfig.PagingType.SCROLL){
//      throw new IllegalStateException("This is not a scroll task");
//    }
//    setScrollId(scrollId);
//    return this;
//  }


}