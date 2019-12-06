/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.apache.spark.dict;

import java.io.Serializable;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode
public class NGlobalDictMetaInfo implements Serializable {

    private int bucketSize;
    private long dictCount;
    private long[] bucketOffsets;
    private long[] bucketCount;

    NGlobalDictMetaInfo(int bucketSize, long[] bucketOffsets, long dictCount, long[] bucketCount) {
        this.bucketSize = bucketSize;
        this.dictCount = dictCount;
        this.bucketOffsets = bucketOffsets;
        this.bucketCount = bucketCount;
    }

    long getOffset(int point) {
        return bucketOffsets[point];
    }

    public int getBucketSize() {
        return bucketSize;
    }

    public void setBucketSize(int bucketSize) {
        this.bucketSize = bucketSize;
    }
}