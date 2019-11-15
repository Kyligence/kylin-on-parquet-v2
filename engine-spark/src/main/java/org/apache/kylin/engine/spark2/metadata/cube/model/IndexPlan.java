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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.engine.spark2.common.obf.IKeep;
import org.apache.kylin.engine.spark2.common.persistence.RootPersistentEntity;
import org.apache.kylin.engine.spark2.metadata.project.NProjectManager;
import org.apache.kylin.engine.spark2.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.IEngineAware;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class IndexPlan extends RootPersistentEntity implements Serializable, IEngineAware, IKeep {
    public static final String INDEX_PLAN_RESOURCE_ROOT = "/index_plan";

    private KylinConfigExt config = null;
    private String project;
    private long prjMvccWhenConfigInitted = -1;
    private long indexPlanMvccWhenConfigInitted = -1;

    @JsonProperty("override_properties")
    private LinkedHashMap<String, String> overrideProps = Maps.newLinkedHashMap();

    public void setProject(String project) {
        this.project = project;
    }

    public void setConfig(KylinConfigExt config) {
        this.config = config;
    }

    public void setPrjMvccWhenConfigInitted(long prjMvccWhenConfigInitted) {
        this.prjMvccWhenConfigInitted = prjMvccWhenConfigInitted;
    }

    public void setIndexPlanMvccWhenConfigInitted(long indexPlanMvccWhenConfigInitted) {
        this.indexPlanMvccWhenConfigInitted = indexPlanMvccWhenConfigInitted;
    }

    @Override
    public int getEngineType() {
        return 0;
    }

    public KylinConfig getConfig() {
        if (config == null) {
            return null;
        }

        ProjectInstance ownerPrj = NProjectManager.getInstance(config).getProject(project);
        if (ownerPrj.getMvcc() != prjMvccWhenConfigInitted || this.getMvcc() != indexPlanMvccWhenConfigInitted) {
            initConfig4IndexPlan(this.config);
        }
        return config;
    }

    private void initConfig4IndexPlan(KylinConfig config) {

        Map<String, String> newOverrides = Maps.newLinkedHashMap(this.overrideProps);
        ProjectInstance ownerPrj = NProjectManager.getInstance(config).getProject(project);
        // cube inherit the project override props
        Map<String, String> prjOverrideProps = ownerPrj.getOverrideKylinProps();
        for (Map.Entry<String, String> entry : prjOverrideProps.entrySet()) {
            if (!newOverrides.containsKey(entry.getKey())) {
                newOverrides.put(entry.getKey(), entry.getValue());
            }
        }

        this.config = KylinConfigExt.createInstance(config, newOverrides);
        this.prjMvccWhenConfigInitted = ownerPrj.getMvcc();
        this.indexPlanMvccWhenConfigInitted = this.getMvcc();
    }

    public static String concatResourcePath(String name, String project) {
        return new StringBuilder().append("/").append(project).append(INDEX_PLAN_RESOURCE_ROOT).append("/").append(name)
                .append(MetadataConstants.FILE_SURFIX).toString();
    }
}
