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

package org.apache.kylin.engine.spark2.metadata.cachesync;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.engine.spark2.common.persistence.JsonSerializer;
import org.apache.kylin.engine.spark2.common.persistence.RawResource;
import org.apache.kylin.engine.spark2.common.persistence.ResourceStore;
import org.apache.kylin.engine.spark2.common.persistence.RootPersistentEntity;
import org.apache.kylin.engine.spark2.common.persistence.Serializer;
import org.apache.kylin.engine.spark2.metadata.util.BrokenEntityProxy;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public abstract class CachedCrudAssist <T extends RootPersistentEntity> {
    private static final Logger logger = LoggerFactory.getLogger(CachedCrudAssist.class);

    private final ResourceStore store;

    private boolean checkCopyOnWrite;

    private final Class<T> entityType;
    private final String resRootPath;
    private final String resPathSuffix;
    private final Serializer<T> serializer;

    private final Cache<String, T> cache;

    protected Cache<String, T> getCache() {
        return cache;
    }

    private final CacheReloadChecker<T> checker;

    public CachedCrudAssist(ResourceStore store, String resourceRootPath, Class<T> entityType) {
        this(store, resourceRootPath, MetadataConstants.FILE_SURFIX, entityType);
    }

    public CachedCrudAssist(ResourceStore store, String resourceRootPath, String resourcePathSuffix,
                            Class<T> entityType) {
        this.store = store;
        this.entityType = entityType;
        this.resRootPath = resourceRootPath;
        this.resPathSuffix = resourcePathSuffix;
        this.serializer = new JsonSerializer<>(entityType);
        this.cache = CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();
        this.checker = new CacheReloadChecker<>(store, this);

        this.checkCopyOnWrite = store.getConfig().isCheckCopyOnWrite();

        Preconditions.checkArgument(resourceRootPath.equals("") || resRootPath.startsWith("/"));
        Preconditions.checkArgument(!resRootPath.endsWith("/"));
    }

    abstract protected T initEntityAfterReload(T entity, String resourceName);

    public T get(String resourceName) {
        RawResource raw = store.getResource(resourcePath(resourceName));
        if (raw == null) {
            cache.invalidate(resourceName);
            return null;
        }
        if (checker.needReload(resourceName)) {
            reloadAt(resourcePath(resourceName));
        }
        return cache.getIfPresent(resourceName);
    }

    String resourcePath(String resourceName) {
        if (StringUtils.isEmpty(resourceName) || StringUtils.containsWhitespace(resourceName)) {
            logger.error("the resourceName \"{}\" cannot contain white character", resourceName);
            throw new IllegalArgumentException(
                    "the resourceName \"" + resourceName + "\" cannot contain white character");
        }
        return resRootPath + "/" + resourceName + resPathSuffix;
    }

    public T reloadAt(String path) {
        T entity = null;
        try {

            entity = store.getResource(path, serializer);
            if (entity == null) {
                throw new IllegalStateException(
                        "No " + entityType.getSimpleName() + " found at " + path + ", returning null");
            }

            // mark cached object
            entity.setCachedAndShared(true);
            entity = initEntityAfterReload(entity, resourceName(path));

            if (!path.equals(resourcePath(entity.resourceName())))
                throw new IllegalStateException("The entity " + entity + " read from " + path
                        + " will save to a different path " + resourcePath(entity.resourceName()));
        } catch (Exception e) {
            logger.warn(
                    "Error loading " + entityType.getSimpleName() + " at " + path + " entity, return a BrokenEntity",
                    e);
            entity = initBrokenEntity(entity, resourceName(path));
        }

        cache.put(entity.resourceName(), entity);
        return entity;
}

    private String resourceName(String resourcePath) {
        Preconditions.checkArgument(resourcePath.startsWith(resRootPath));
        Preconditions.checkArgument(resourcePath.endsWith(resPathSuffix));
        return resourcePath.substring(resRootPath.length() + 1, resourcePath.length() - resPathSuffix.length());
    }

    protected T initBrokenEntity(T entity, String resourceName) {
        String resourcePath = resourcePath(resourceName);

        T brokenEntity = BrokenEntityProxy.getProxy(entityType, resourcePath);
        brokenEntity.setUuid(resourceName);

        if (entity != null)
            brokenEntity.setMvcc(entity.getMvcc());

        return brokenEntity;
    }

    public void setCheckCopyOnWrite(boolean check) {
        this.checkCopyOnWrite = check;
    }
}
