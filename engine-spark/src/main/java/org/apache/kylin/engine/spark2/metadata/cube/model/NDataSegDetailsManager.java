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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark2.common.obf.IKeepNames;
import org.apache.kylin.engine.spark2.common.persistence.JsonSerializer;
import org.apache.kylin.engine.spark2.common.persistence.ResourceStore;
import org.apache.kylin.engine.spark2.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;

public class NDataSegDetailsManager implements IKeepNames {
    private static final Serializer<NDataSegDetails> DATA_SEG_LAYOUT_INSTANCES_SERIALIZER = new JsonSerializer<>(
            NDataSegDetails.class);

    private KylinConfig kylinConfig;
    private String project;

    public static NDataSegDetailsManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NDataSegDetailsManager.class);
    }

    NDataSegDetails getForSegment(NDataSegment segment) {
        return getForSegment(segment.getDataflow(), segment.getId());
    }

    private NDataSegDetails getForSegment(NDataflow df, String segId) {
        NDataSegDetails instances = getStore().getResource(getResourcePathForSegment(df.getUuid(), segId),
                DATA_SEG_LAYOUT_INSTANCES_SERIALIZER);
        if (instances != null) {
            instances.setConfig(df.getConfig());
            instances.setProject(project);
        }
        return instances;
    }


    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.kylinConfig);
    }

    private String getResourcePathForSegment(String dfId, String segId) {
        return getResourcePathForDetails(dfId) + "/" + segId + MetadataConstants.FILE_SURFIX;
    }

    private String getResourcePathForDetails(String dfId) {
        return "/" + project + NDataSegDetails.DATAFLOW_DETAILS_RESOURCE_ROOT + "/" + dfId;
    }

}
