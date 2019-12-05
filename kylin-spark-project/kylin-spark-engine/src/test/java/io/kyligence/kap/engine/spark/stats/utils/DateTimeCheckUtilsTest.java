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

package io.kyligence.kap.engine.spark.stats.utils;

import org.junit.Assert;
import org.junit.Test;

public class DateTimeCheckUtilsTest {

    @Test
    public void testBasic() {
        Assert.assertTrue(DateTimeCheckUtils.isDate("2018-01-01"));
        Assert.assertFalse(DateTimeCheckUtils.isDate("2018"));
        Assert.assertFalse(DateTimeCheckUtils.isDate("01-01"));

        Assert.assertTrue(DateTimeCheckUtils.isTime("01:01:00"));
        Assert.assertFalse(DateTimeCheckUtils.isTime("aabb"));
    }
}
