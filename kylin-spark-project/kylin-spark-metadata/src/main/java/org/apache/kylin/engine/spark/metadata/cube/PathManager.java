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

package org.apache.kylin.engine.spark.metadata.cube;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PathManager {
    private static final Logger logger = LoggerFactory.getLogger(PathManager.class);

    public static String getCubePath(String projectName, String cubeId) {
        return "/" + projectName + ResourceStore.CUBE_RESOURCE_ROOT + "/" + cubeId + MetadataConstants.FILE_SURFIX;
    }

    public static String getModelPath(String projectName, String cubeId) {
        return new StringBuilder().append("/").append(projectName).append(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT)
                .append("/").append(cubeId).append(MetadataConstants.FILE_SURFIX).toString();
    }

    public static String getProjectPath(String projectName) {
        return ResourceStore.PROJECT_RESOURCE_ROOT + "/" + projectName + MetadataConstants.FILE_SURFIX;
    }

    public static String resourcePath(String resRootPath, String resourceName) {
        if (StringUtils.isEmpty(resourceName) || StringUtils.containsWhitespace(resourceName)) {
            logger.error("the resourceName \"{}\" cannot contain white character", resourceName);
            throw new IllegalArgumentException(
                    "the resourceName \"" + resourceName + "\" cannot contain white character");
        }
        return resRootPath + "/" + resourceName + MetadataConstants.FILE_SURFIX;
    }
}
