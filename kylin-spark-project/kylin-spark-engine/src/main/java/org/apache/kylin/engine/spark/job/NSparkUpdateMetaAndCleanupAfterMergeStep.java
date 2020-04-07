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

package org.apache.kylin.engine.spark.job;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.spark.merger.AfterMergeOrRefreshResourceMerger;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.ExecutableContext;

import org.apache.kylin.metadata.MetadataConstants;

public class NSparkUpdateMetaAndCleanupAfterMergeStep extends NSparkExecutable {
    public NSparkUpdateMetaAndCleanupAfterMergeStep() {
        this.setName(ExecutableConstants.STEP_NAME_MERGE_CLEANUP);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        String cubeId = getParam(MetadataConstants.P_CUBE_ID);
        String[] segments = StringUtils.split(getParam(MetadataConstants.P_SEGMENT_NAMES), ",");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        CubeInstance cube = CubeManager.getInstance(config).getCubeByUuid(cubeId);

        updateMetadataAfterMerge(cubeId);

        for (String segmentName : segments) {
            String path = config.getHdfsWorkingDirectory() + cube.getProject() + "/parquet/" + cube.getName() + "/" + segmentName;
            try {
                HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), new Path(path));
            } catch (IOException e) {
                throw new ExecuteException("Can not delete segment: " + segmentName + ", in cube: " + cube.getName());
            }
        }

        return ExecuteResult.createSucceed();
    }

    private void updateMetadataAfterMerge(String cubeId) {
        String buildStepUrl = getParam(MetadataConstants.P_OUTPUT_META_URL);
        KylinConfig buildConfig = KylinConfig.createKylinConfig(this.getConfig());
        buildConfig.setMetadataUrl(buildStepUrl);
        ResourceStore resourceStore = ResourceStore.getStore(buildConfig);
        String mergedSegmentId = getParam(CubingExecutableUtil.SEGMENT_ID);
        AfterMergeOrRefreshResourceMerger merger = new AfterMergeOrRefreshResourceMerger(buildConfig);
        merger.merge(cubeId, mergedSegmentId, resourceStore, getParam(MetadataConstants.P_JOB_TYPE));
    }
}
