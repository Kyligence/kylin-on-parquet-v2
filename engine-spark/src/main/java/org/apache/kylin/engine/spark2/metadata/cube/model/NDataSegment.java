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

package org.apache.kylin.engine.spark2.metadata.cube.model;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NDataSegment implements ISegment {

    @JsonProperty("segRange")
    private SegmentRange segmentRange;

    @JsonBackReference
    private NDataflow dataflow;

    @JsonProperty("id")
    private String id; // Sequence ID within NDataflow

    // computed fields below

    private transient NDataSegDetails segDetails; // transient, not required by spark cubing
    private transient Map<Long, NDataLayout> layoutsMap = Collections.emptyMap(); // transient, not required by spark cubing


    void initAfterReload() {
        segDetails = NDataSegDetailsManager.getInstance(getConfig(), dataflow.getProject()).getForSegment(this);
        if (segDetails == null) {
            segDetails = NDataSegDetails.newSegDetails(dataflow, id);
        }

        segDetails.setCachedAndShared(dataflow.isCachedAndShared());

        List<NDataLayout> cuboids = segDetails.getLayouts();
        layoutsMap = new HashMap<>(cuboids.size());
        for (NDataLayout i : cuboids) {
            layoutsMap.put(i.getLayoutId(), i);
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public NDataflow getDataflow() {
        return dataflow;
    }

    @Override
    public KylinConfig getConfig() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public boolean isOffsetCube() {
        return false;
    }

    @Override
    public SegmentRange getSegRange() {
        return segmentRange;
    }

    @Override
    public SegmentRange.TSRange getTSRange() {
        return null;
    }

    @Override
    public DataModelDesc getModel() {
        return null;
    }

    @Override
    public SegmentStatusEnum getStatus() {
        return null;
    }

    @Override
    public long getLastBuildTime() {
        return 0;
    }

    @Override
    public void validate() throws IllegalStateException {

    }

    @Override
    public int compareTo(ISegment o) {
        return 0;
    }

    public NDataSegDetails getSegDetails() {
        return segDetails;
    }

    public void setSegmentRange(SegmentRange segmentRange) {
        checkIsNotCachedAndShared();
        this.segmentRange = segmentRange;
    }

    public void checkIsNotCachedAndShared() {
        if (isCachedAndShared())
            throw new IllegalStateException();
    }

    public boolean isCachedAndShared() {
        if (dataflow == null || !dataflow.isCachedAndShared())
            return false;

        for (NDataSegment cached : dataflow.getSegments()) {
            if (cached == this)
                return true;
        }
        return false;
    }
}
