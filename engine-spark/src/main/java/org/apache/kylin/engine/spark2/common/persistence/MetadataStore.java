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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.ClassUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NavigableSet;

public abstract class MetadataStore {
    private static final Logger log = LoggerFactory.getLogger(MetadataStore.class);

    public static MetadataStore createMetadataStore(KylinConfig config) {
        StorageURL url = config.getMetadataUrl();
        log.info("Creating metadata store by KylinConfig {}", config);
        //String clsName = config.getMetadataStoreImpls().get(url.getScheme());
        String clsName = config.getResourceStoreImpls().get(url.getScheme());
        try {
            Class<? extends MetadataStore> cls = ClassUtil.forName(clsName, MetadataStore.class);
            return cls.getConstructor(KylinConfig.class).newInstance(config);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to create metadata store", e);
        }
    }

    public void restore(ResourceStore store) throws IOException {
        NavigableSet<String> all = list("/");
        for (String resPath : all) {
            RawResource raw = load(resPath);
            store.putResourceWithoutCheck(resPath, raw.getByteSource(), raw.getTimestamp(), raw.getMvcc());
        }
    }

    public abstract RawResource load(String path) throws IOException;

    public abstract NavigableSet<String> list(String rootPath);
}
