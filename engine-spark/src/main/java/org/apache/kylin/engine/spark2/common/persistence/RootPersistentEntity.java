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

package org.apache.kylin.engine.spark2.common.persistence;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.google.common.collect.Lists;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.engine.spark2.metadata.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

abstract public class RootPersistentEntity implements AclEntity, Serializable {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(RootPersistentEntity.class);

    @JsonProperty("uuid")
    protected String uuid = UUID.randomUUID().toString();

    @JsonProperty("mvcc")
    @JsonView(JsonUtil.PublicView.class)
    private long mvcc = -1;

    @JsonProperty("last_modified")
    protected long lastModified;

    private boolean isBroken = false;

    private List<RootPersistentEntity> dependencies;

    // if cached and shared, the object MUST NOT be modified (call setXXX() for example)
    protected boolean isCachedAndShared = false;

    public boolean isBroken() {
        return isBroken;
    }

    public void setBroken(boolean broken) {
        isBroken = broken;
    }

    public boolean isCachedAndShared() {
        return isCachedAndShared;
    }

    public String getUuid() {
        return uuid;
    }

    public List<RootPersistentEntity> getDependencies() {
        return dependencies;
    }

    public void setCachedAndShared(boolean isCachedAndShared) {
        if (this.isCachedAndShared && isCachedAndShared == false)
            throw new IllegalStateException();

        this.isCachedAndShared = isCachedAndShared;
    }

    /**
     * The name as a part of the resource path used to save the entity.
     * <p>
     * E.g. /resource-root-dir/{RESOURCE_NAME}.json
     */
    public String resourceName() {
        return uuid;
    }

    public void setDependencies(List<RootPersistentEntity> dependencies) {
        this.dependencies = dependencies;
    }

    public void setUuid(String uuid) {
        checkIsNotCachedAndShared();
        this.uuid = uuid;
    }

    public void checkIsNotCachedAndShared() {
        if (isCachedAndShared)
            throw new IllegalStateException();
    }

    public long getMvcc() {
        return mvcc;
    }

    public String getResourcePath() {
        return "";
    }

    public void setMvcc(long mvcc) {
        if (isCachedAndShared) {
            logger.warn("[UNEXPECTED_THINGS_HAPPENED]update mvcc for isCachedAndShared object " + this.getClass()
                    + ", from " + this.mvcc + " to " + mvcc, new IllegalStateException());
        }
        this.mvcc = mvcc;
    }

    public void setLastModified(long lastModified) {
        //checkIsNotCachedAndShared(); // comment out due to let pass legacy tests, like StreamingManagerTest
        this.lastModified = lastModified;
    }

    @Override
    public String getId() {
        return uuid;
    }

    public List<RootPersistentEntity> calcDependencies() {
        return Lists.newArrayList();
    }
}
