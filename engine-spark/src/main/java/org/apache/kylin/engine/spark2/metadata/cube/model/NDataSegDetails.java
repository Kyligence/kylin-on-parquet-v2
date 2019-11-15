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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.engine.spark2.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class NDataSegDetails extends RootPersistentEntity implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(NDataSegDetails.class);

    public static final String DATAFLOW_DETAILS_RESOURCE_ROOT = "/dataflow_details";
    @JsonProperty("dataflow")
    private String dataflowId;
    @JsonManagedReference
    @JsonProperty("layout_instances")
    private List<NDataLayout> layouts = Lists.newArrayList();

    private String project;
    @JsonIgnore
    private KylinConfigExt config;

    @Override
    public String getId() {
        return null;
    }

    public KylinConfigExt getConfig() {
        return config;
    }

    void setConfig(KylinConfigExt config) {
        this.config = config;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getDataflowId() {
        return dataflowId;
    }

    public void setDataflowId(String dataflowId) {
        checkIsNotCachedAndShared();
        this.dataflowId = dataflowId;
    }

    public List<NDataLayout> getLayouts() {
        return layouts;
    }

    public void setLayouts(List<NDataLayout> layouts) {
        checkIsNotCachedAndShared();
        this.layouts = layouts;
    }

    public static NDataSegDetails newSegDetails(NDataflow df, String segId) {
        NDataSegDetails entity = new NDataSegDetails();
        entity.setConfig(df.getConfig());
        entity.setUuid(segId);
        entity.setDataflowId(df.getUuid());
        entity.setProject(df.getProject());

        List<NDataLayout> cuboids = new ArrayList<>();
        entity.setLayouts(cuboids);
        return entity;
    }

    @Override
    public String getResourcePath() {
        return "/" + project + NDataSegDetails.DATAFLOW_DETAILS_RESOURCE_ROOT + "/" + dataflowId + "/" + uuid
                + MetadataConstants.FILE_SURFIX;
    }
}
