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

package io.kyligence.kap.engine.spark.job;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.ExecutableContext;

import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;

public class NSparkCleanupAfterMergeStep extends NSparkExecutable {
    public NSparkCleanupAfterMergeStep() {
        this.setName(ExecutableConstants.STEP_NAME_CLEANUP);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        String name = getParam(NBatchConstants.P_DATAFLOW_ID);
        String[] segmentIds = StringUtils.split(getParam(NBatchConstants.P_SEGMENT_IDS));
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflow dataflow = NDataflowManager.getInstance(config, getProject()).getDataflow(name);

        for (String segmentId : segmentIds) {
            String path = dataflow.getSegmentHdfsPath(segmentId);
            try {
                HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), new Path(path));
            } catch (IOException e) {
                throw new ExecuteException("Can not delete segment: " + segmentId + ", in dataflow: " + name);
            }
        }

        return ExecuteResult.createSucceed();
    }

}
