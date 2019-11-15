/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.kylin.engine.spark2.job;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark2.metadata.cube.model.NDataSegment;
import org.apache.kylin.engine.spark2.metadata.cube.model.LayoutEntity;
import org.apache.kylin.engine.spark2.metadata.cube.model.NDataflow;
import org.apache.kylin.engine.spark2.metadata.cube.model.NDataflowManager;
//import org.apache.kylin.job.execution.DefaultChainedExecutableOnModel;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
//import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.engine.spark2.metadata.cube.model.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.base.Preconditions;

//import io.kyligence.kap.metadata.cube.model.LayoutEntity;
//import io.kyligence.kap.metadata.cube.model.BatchConstants;
/*import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;*/

/**
 */
public class SparkCubingJob extends DefaultChainedExecutable {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(SparkCubingJob.class);

    // for test use only
    public static SparkCubingJob create(Set<NDataSegment> segments, Set<LayoutEntity> layouts, String submitter) {
        return create(segments, layouts, submitter, JobTypeEnum.INDEX_BUILD, UUID.randomUUID().toString());
    }

    public static SparkCubingJob create(Set<NDataSegment> segments, Set<LayoutEntity> layouts, String submitter,
                                        JobTypeEnum jobType, String jobId) {
        Preconditions.checkArgument(!segments.isEmpty());
        Preconditions.checkArgument(!layouts.isEmpty());
        Preconditions.checkArgument(submitter != null);
        NDataflow df = segments.iterator().next().getDataflow();
        SparkCubingJob job = new SparkCubingJob();
        long startTime = Long.MAX_VALUE - 1;
        long endTime = 0L;
        for (NDataSegment segment : segments) {
            startTime = startTime < Long.parseLong(segment.getSegRange().start.toString()) ? startTime
                    : Long.parseLong(segment.getSegRange().start.toString());
            endTime = endTime > Long.parseLong(segment.getSegRange().start.toString()) ? endTime
                    : Long.parseLong(segment.getSegRange().start.toString());
        }
        job.setId(jobId);
        job.setName(jobType.toString());
        job.setJobType(jobType);
        job.setTargetSubject(segments.iterator().next().getModel().getUuid());
        job.setTargetSegments(segments.stream().map(x -> String.valueOf(x.getId())).collect(Collectors.toList()));
        job.setProject(df.getProject());
        job.setSubmitter(submitter);

        job.setParam(BatchConstants.P_JOB_ID, jobId);
        job.setParam(BatchConstants.P_PROJECT_NAME, df.getProject());
        job.setParam(BatchConstants.P_TARGET_MODEL, job.getTargetSubject());
        job.setParam(BatchConstants.P_DATAFLOW_ID, df.getId());
        job.setParam(BatchConstants.P_LAYOUT_IDS, NSparkCubingUtil.ids2Str(NSparkCubingUtil.toLayoutIds(layouts)));
        job.setParam(BatchConstants.P_SEGMENT_IDS, String.join(",", job.getTargetSegments()));
        job.setParam(BatchConstants.P_DATA_RANGE_START, String.valueOf(startTime));
        job.setParam(BatchConstants.P_DATA_RANGE_END, String.valueOf(endTime));

        JobStepFactory.addStep(job, JobStepType.RESOURCE_DETECT, segments);
        JobStepFactory.addStep(job, JobStepType.CUBING, segments);
        return job;
    }

    @Override
    public Set<String> getMetadataDumpList(KylinConfig config) {
        final String dataflowId = getParam(BatchConstants.P_DATAFLOW_ID);
        return NDataflowManager.getInstance(config, getProject()) //
                .getDataflow(dataflowId) //
                .collectPrecalculationResource();
    }

    public SparkCubingStep getSparkCubingStep() {
        return getTask(SparkCubingStep.class);
    }

    ResourceDetectStep getResourceDetectStep() {
        return getTask(ResourceDetectStep.class);
    }

    /*@Override
    public void cancelJob() {
        NDataflowManager nDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        NDataflow dataflow = nDataflowManager.getDataflow(getSparkCubingStep().getDataflowId());
        List<NDataSegment> segments = new ArrayList<>();
        for (String id : getSparkCubingStep().getSegmentIds()) {
            NDataSegment segment = dataflow.getSegment(id);
            if (segment != null && !segment.getStatus().equals(SegmentStatusEnum.READY)) {
                segments.add(segment);
            }
        }
        NDataSegment[] segmentsArray = new NDataSegment[segments.size()];
        NDataSegment[] nDataSegments = segments.toArray(segmentsArray);
        NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        nDataflowUpdate.setToRemoveSegs(nDataSegments);
        nDataflowManager.updateDataflow(nDataflowUpdate);
        NDefaultScheduler.stopThread(getId());
    }*/

}
