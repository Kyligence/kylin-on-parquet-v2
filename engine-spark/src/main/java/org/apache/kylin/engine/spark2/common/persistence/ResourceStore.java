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

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSource;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ResourceStore implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ResourceStore.class);

    private static final Map<KylinConfig, ResourceStore> META_CACHE = new ConcurrentHashMap<>();

    protected MetadataStore metadataStore;
    /**
     * Read a resource, return null in case of not found or is a folder.
     */
    public final <T extends RootPersistentEntity> T getResource(String resPath, Serializer<T> serializer) {
        resPath = norm(resPath);
        RawResource res = getResourceImpl(resPath);
        if (res == null)
            return null;

        return getResourceFromRawResource(res, serializer);
    }

    protected final KylinConfig kylinConfig;

    protected ResourceStore(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }


    public final KylinConfig getConfig() {
        return kylinConfig;
    }

    public final RawResource getResource(String resPath) {
        return getResourceImpl(norm(resPath));
    }

    public MetadataStore getMetadataStore() {
        return metadataStore;
    }

    protected void init(MetadataStore metadataStore) throws Exception {
        metadataStore.restore(this);
        this.metadataStore = metadataStore;
    }

    public void putResourceWithoutCheck(String resPath, ByteSource bs, long timeStamp, long newMvcc) {
        throw new NotImplementedException("Only implemented in InMemoryResourceStore");
    }

    private <T extends RootPersistentEntity> T getResourceFromRawResource(RawResource res, Serializer<T> serializer) {
        try (InputStream is = res.getByteSource().openStream(); DataInputStream din = new DataInputStream(is)) {
            T r = serializer.deserialize(din);
            r.setLastModified(res.getTimestamp());
            r.setMvcc(res.getMvcc());
            return r;
        } catch (IOException e) {
            logger.warn("error when deserializing resource: " + res.getResPath(), e);
            return null;
        }
    }

    private String norm(String resPath) {
        resPath = resPath.trim();
        while (resPath.startsWith("//"))
            resPath = resPath.substring(1);
        while (resPath.endsWith("/"))
            resPath = resPath.substring(0, resPath.length() - 1);
        if (!resPath.startsWith("/"))
            resPath = "/" + resPath;

        Preconditions.checkArgument(!resPath.contains("//"),
                String.format("input resPath contains consequent slash: %s", resPath));

        return resPath;
    }

    /**
     * returns null if not exists
     */
    protected abstract RawResource getResourceImpl(String resPath);


    /**
     * Get a resource store for Kylin's metadata.
     */
    public static ResourceStore getKylinMetaStore(KylinConfig config) {
        ResourceStore store = META_CACHE.get(config);
        if (store != null)
            return store;

        synchronized (ResourceStore.class) {
            store = META_CACHE.get(config);
            if (store == null) {
                store = createResourceStore(config);
                META_CACHE.put(config, store);

                if (isPotentialMemoryLeak()) {
                    logger.warn("Cached {} kylin meta stores, memory leak?", META_CACHE.size(), new RuntimeException());
                }
            }
        }
        return store;
    }

    public static boolean isPotentialMemoryLeak() {
        return META_CACHE.size() > 100;
    }

    /**
     * Create a resource store for general purpose, according specified by given StorageURL.
     */
    private static ResourceStore createResourceStore(KylinConfig config) {
        try (InMemResourceStore resourceStore = new InMemResourceStore(config)) {
            MetadataStore snapshotStore = MetadataStore.createMetadataStore(config);
            resourceStore.init(snapshotStore);
            return resourceStore;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to create metadata store", e);
        }
    }

    public void close() {
        clearCache(this.getConfig());
    }

    public static void clearCache(KylinConfig config) {
        META_CACHE.remove(config);
    }
}
