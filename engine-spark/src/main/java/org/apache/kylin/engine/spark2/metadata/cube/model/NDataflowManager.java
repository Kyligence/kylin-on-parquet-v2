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

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.engine.spark2.common.obf.IKeepNames;
import org.apache.kylin.engine.spark2.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.engine.spark2.common.persistence.ResourceStore;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationProvider;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NDataflowManager implements IRealizationProvider, IKeepNames {
    private static final Logger logger = LoggerFactory.getLogger(NDataflowManager.class);
    private CachedCrudAssist<NDataflow> crud;
    private KylinConfig config;
    private String project;

    public static NDataflowManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NDataflowManager.class);
    }

    private NDataflowManager(KylinConfig cfg, final String project) {
        /*if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing NDataflowManager with KylinConfig Id: {} for project {}",
                    System.identityHashCode(cfg), project);*/
        this.config = cfg;
        this.project = project;
        String resourceRootPath = "/" + project + NDataflow.DATAFLOW_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<NDataflow>(getStore(), resourceRootPath, NDataflow.class) {
            @Override
            protected NDataflow initEntityAfterReload(NDataflow df, String resourceName) {
                IndexPlan plan = NIndexPlanManager.getInstance(config, project).getIndexPlan(df.getUuid());
                df.initAfterReload((KylinConfigExt) plan.getConfig(), project);
                return df;
            }

            @Override
            protected NDataflow initBrokenEntity(NDataflow entity, String resourceName) {
                NDataflow dataflow = super.initBrokenEntity(entity, resourceName);
                IndexPlan plan = NIndexPlanManager.getInstance(config, project).getIndexPlan(resourceName);
                dataflow.setConfig((KylinConfigExt) plan.getConfig());
                dataflow.setProject(project);
                dataflow.setDependencies(dataflow.calcDependencies());
                return dataflow;
            }
        };
        this.crud.setCheckCopyOnWrite(true);
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }


    public NDataflow getDataflow(String id) {
        if (StringUtils.isEmpty(id)) {
            return null;
        }
        return crud.get(id);
    }

    @Override
    public RealizationType getRealizationType() {
        return null;
    }

    @Override
    public IRealization getRealization(String name) {
        return null;
    }
}
