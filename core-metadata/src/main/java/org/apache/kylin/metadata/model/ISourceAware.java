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

package org.apache.kylin.metadata.model;

import org.apache.kylin.common.KylinConfig;

public interface ISourceAware {

    public static final int ID_HIVE = 0;
    public static final int ID_STREAMING = 1;
    public static final int ID_SPARKSQL = 5;
    public static final int ID_EXTERNAL = 7;
    public static final int ID_JDBC = 8;
    public static final int ID_SPARK = 9;
    public static final int ID_EXTENSIBLE_JDBC = 16;
    public static final int ID_KAFKA = 20;
    public static final int ID_KAFKA_HIVE = 21;

    int getSourceType();

    KylinConfig getConfig();
}
