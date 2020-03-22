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

package org.apache.kylin.query.pushdown;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.metadata.cube.StructField;
import org.apache.spark.sql.SparderContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class SparkSubmitter {
    public static final Logger logger = LoggerFactory.getLogger(SparkSubmitter.class);

    public static PushdownResponse submitPushDownTask(String sql) {
        SparkSession ss = SparderContext.getSparkSession();
        Pair<List<List<String>>, List<StructField>> pair = SparkSqlClient.executeSql(ss, sql, UUID.randomUUID());
        return new PushdownResponse(pair.getSecond(), pair.getFirst());
    }

}
