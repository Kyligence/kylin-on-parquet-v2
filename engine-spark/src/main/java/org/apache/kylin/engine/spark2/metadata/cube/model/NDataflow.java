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
import org.apache.kylin.engine.spark2.common.persistence.MissingRootPersistentEntity;
import org.apache.kylin.engine.spark2.common.persistence.RootPersistentEntity;
import org.apache.kylin.engine.spark2.metadata.realization.IRealization;
import org.apache.kylin.engine.spark2.metadata.model.NDataModel;
import org.apache.kylin.engine.spark2.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableRef;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class NDataflow extends RootPersistentEntity implements IRealization {
    public static final String REALIZATION_TYPE = "NCUBE";
    public static final String DATAFLOW_RESOURCE_ROOT = "/dataflow";

    private String project;

    @JsonManagedReference
    @JsonProperty("segments")
    private Segments<NDataSegment> segments = new Segments<NDataSegment>();

    @JsonIgnore
    private KylinConfigExt config;

    public void setConfig(KylinConfigExt config) {
        this.config = config;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public Set<String> collectPrecalculationResource() {
        Set<String> r = new LinkedHashSet<>();

        // dataflow & segments
        r.add(this.getResourcePath());
        for (NDataSegment seg : segments) {
            r.add(seg.getSegDetails().getResourcePath());
        }

        // cubing plan
        r.add(getIndexPlan().getResourcePath());

        // project & model & tables
        r.add(getModel().getProjectInstance().getResourcePath());
        r.add(getModel().getResourcePath());
        for (TableRef t : getModel().getAllTables()) {
            r.add(t.getTableDesc().getResourcePath());
        }

        return r;
    }

    public void initAfterReload(KylinConfigExt config, String project) {
        this.project = project;
        this.config = config;
        for (NDataSegment seg : segments) {
            seg.initAfterReload();
        }

        this.setDependencies(calcDependencies());

    }

    public KylinConfigExt getConfig() {
        return (KylinConfigExt) getIndexPlan().getConfig();
    }

    @Override
    public NDataModel getModel() {
        return NDataModelManager.getInstance(config, project).getDataModelDesc(uuid);
    }

    public Segments<NDataSegment> getSegments() {
        return isCachedAndShared() ? new Segments(segments) : segments;
    }

    public IndexPlan getIndexPlan() {
        return NIndexPlanManager.getInstance(config, project).getIndexPlan(uuid);
    }

    @Override
    public String getResourcePath() {
        return concatResourcePath(getUuid(), project);
    }

    public static String concatResourcePath(String name, String project) {
        return "/" + project + DATAFLOW_RESOURCE_ROOT + "/" + name + MetadataConstants.FILE_SURFIX;
    }

    @Override
    public List<RootPersistentEntity> calcDependencies() {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(config, project);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(getId());

        return Lists.newArrayList(indexPlan != null ? indexPlan
                : new MissingRootPersistentEntity(IndexPlan.concatResourcePath(getId(), project)));
    }

}
