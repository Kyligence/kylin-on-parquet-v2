/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.apache.kylin.engine.spark2.metadata.cachesync;

import com.google.common.base.Preconditions;
import org.apache.kylin.engine.spark2.common.persistence.MissingRootPersistentEntity;
import org.apache.kylin.engine.spark2.common.persistence.RawResource;
import org.apache.kylin.engine.spark2.common.persistence.ResourceStore;
import org.apache.kylin.engine.spark2.common.persistence.RootPersistentEntity;

import java.util.List;

public class CacheReloadChecker<T extends RootPersistentEntity> {

    private ResourceStore store;

    private CachedCrudAssist<T> crud;

    public CacheReloadChecker(ResourceStore store, CachedCrudAssist<T> crud) {
        this.store = store;
        this.crud = crud;
    }

    boolean needReload(String resourceName) {
        RootPersistentEntity entity = crud.getCache().getIfPresent(resourceName);
        if (entity == null) {
            return true;
        } else {
            return checkDependencies(entity);
        }
    }

    private boolean checkDependencies(RootPersistentEntity entity) {
        RawResource raw = store.getResource(entity.getResourcePath());
        if (raw == null) {
            // if still missing, no need to reload
            return !(entity instanceof MissingRootPersistentEntity);
        }
        if (raw.getMvcc() != entity.getMvcc()) {
            return true;
        }

        Preconditions.checkState(!(entity instanceof MissingRootPersistentEntity));

        List<RootPersistentEntity> entities = entity.getDependencies();
        if (entities == null) {
            return false;
        }
        for (RootPersistentEntity depEntity : entities) {
            boolean depNeedReload = checkDependencies(depEntity);
            if (depNeedReload) {
                return true;
            }
        }
        return false;
    }

}
