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

package io.kyligence.kap.engine.spark.merger;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import com.clearspring.analytics.util.Lists;
import org.apache.kylin.metadata.model.Segments;

public class AfterMergeOrRefreshResourceMerger extends MetadataMerger {

    public AfterMergeOrRefreshResourceMerger(KylinConfig config) {
        super(config);
    }

    @Override
    public void merge(String cubeId, Set<String> segmentIds, ResourceStore remoteResourceStore, String jobType) {

        CubeManager cubeManager = CubeManager.getInstance(getConfig());
        CubeInstance cubeInstance = cubeManager.getCubeByUuid(cubeId);
        CubeUpdate update = new CubeUpdate(cubeInstance.latestCopyForWrite());

        CubeManager distManager = CubeManager.getInstance(remoteResourceStore.getConfig());
        CubeInstance distCube = distManager.getCubeByUuid(cubeId).latestCopyForWrite();

        List<CubeSegment> toUpdateSegments = Lists.newArrayList();

        CubeSegment mergedSegment = distCube.getSegmentById(segmentIds.iterator().next());
        mergedSegment.setStatus(SegmentStatusEnum.READY);

        toUpdateSegments.add(mergedSegment);
        if (String.valueOf(JobTypeEnum.INDEX_REFRESH).equals(jobType)) {
            //TODO: update snapshot
            //updateSnapshotTableIfNeed(mergedSegment);
        }

        List<CubeSegment> toRemoveSegments = getToRemoveSegs(distCube, mergedSegment);
        if (String.valueOf(JobTypeEnum.INDEX_MERGE).equals(jobType)) {
            Optional<Long> reduce = toRemoveSegments.stream()
                    .map(CubeSegment::getSizeKB)
                    .filter(size -> size != -1)
                    .reduce(Long::sum);
            if (reduce.isPresent()) {
                long totalSourceSize = reduce.get();
                mergedSegment.setSizeKB(totalSourceSize);
                mergedSegment.setLastBuildTime(System.currentTimeMillis());
            }
        }

        update.setToRemoveSegs(toRemoveSegments.toArray(new CubeSegment[0]));
        update.setToUpdateSegs(toUpdateSegments.toArray(new CubeSegment[0]));

        try {
            cubeManager.updateCube(update);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    List<CubeSegment> getToRemoveSegs(CubeInstance cube, CubeSegment mergedSegment) {
        Segments tobe = cube.calculateToBeSegments(mergedSegment);

        if (!tobe.contains(mergedSegment))
            throw new IllegalStateException(
                    "For Cube " + cube + ", segment " + mergedSegment + " is expected but not in the tobe " + tobe);

        if (mergedSegment.getStatus() == SegmentStatusEnum.NEW)
            mergedSegment.setStatus(SegmentStatusEnum.READY);

        List<CubeSegment> toRemoveSegs = Lists.newArrayList();
        for (CubeSegment s : cube.getSegments()) {
            if (!tobe.contains(s))
                toRemoveSegs.add(s);
        }

        return toRemoveSegs;
    }

    @Override
    public void merge(AbstractExecutable abstractExecutable) {
        String buildStepUrl = abstractExecutable.getParam(MetadataConstants.P_OUTPUT_META_URL);
        KylinConfig buildConfig = KylinConfig.createKylinConfig(this.getConfig());
        buildConfig.setMetadataUrl(buildStepUrl);
        ResourceStore resourceStore = ResourceStore.getStore(buildConfig);
        String cubeId = abstractExecutable.getParam(MetadataConstants.P_CUBE_ID);
        Set<String> segmentIds = Stream.of(StringUtils.split(abstractExecutable.getParam(MetadataConstants.P_SEGMENT_IDS), ","))
                .collect(Collectors.toSet());
        merge(cubeId, segmentIds, resourceStore, abstractExecutable.getParam(MetadataConstants.P_JOB_TYPE));
        //recordDownJobStats(abstractExecutable, nDataLayouts);
        //abstractExecutable.notifyUserIfNecessary(nDataLayouts);

    }

}
