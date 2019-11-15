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

package org.apache.kylin.engine.spark2.metadata.util;

import com.google.common.collect.Sets;
import org.springframework.cglib.proxy.Enhancer;
import org.apache.kylin.engine.spark2.common.persistence.RootPersistentEntity;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;

import java.lang.reflect.Method;
import java.util.Set;

public class BrokenEntityProxy implements MethodInterceptor {

    private static Set<String> methods = Sets.newHashSet("setBroken", "isBroken", "setProject", "getProject",
            "resourceName", "getId", "getUuid", "setUuid", "getAlias", "setAlias", "checkBrokenWithRelatedInfo",
            "toString", "getMvcc", "setMvcc", "setConfig", "getConfig", "getModelAlias", "getModel",
            "getRootFactTableName", "setRootFactTableName", "getJoinTables", "setJoinTables", "calcDependencies",
            "getDependencies", "setDependencies", "setHandledAfterBroken", "isHandledAfterBroken");

    private final String resourcePath;

    public BrokenEntityProxy(String resourcePath) {
        this.resourcePath = resourcePath;
    }

    public static <T extends RootPersistentEntity> T getProxy(Class<T> cls, String resourcePath) {
        BrokenEntityProxy proxy = new BrokenEntityProxy(resourcePath);
        T brokenEntity = (T) Enhancer.create(cls, proxy);
        brokenEntity.setBroken(true);
        brokenEntity.setMvcc(-1L);
        return brokenEntity;
    }

    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        if (methods.contains(method.getName())) {
            return proxy.invokeSuper(obj, args);
        }
        if (method.getName().equals("getResourcePath")) {
            return resourcePath;
        }

        if (method.getName().equals("checkIsNotCachedAndShared")) {
            return null;
        }
        throw new RuntimeException("call on Broken Entity's " + method.getName() + " method");
    }

}
