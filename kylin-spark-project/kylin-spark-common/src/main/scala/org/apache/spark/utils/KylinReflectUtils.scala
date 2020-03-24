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
package org.apache.spark.utils

import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.{SPARK_VERSION, SparkContext}
import org.apache.spark.util.Utils

import scala.reflect.runtime._

object KylinReflectUtils {
  private val rm = universe.runtimeMirror(getClass.getClassLoader)

  def getSessionState(sparkContext: SparkContext, kylinSession: Object): Any = {
    if (SPARK_VERSION.startsWith("2.4")) {
      var className: String =
        "org.apache.spark.sql.hive.KylinHiveSessionStateBuilder"
      if (!"hive".equals(sparkContext.getConf
        .get(CATALOG_IMPLEMENTATION.key, "in-memory"))) {
        className = "org.apache.spark.sql.hive.KylinSessionStateBuilder"
      }
      val tuple = createObject(className, kylinSession, None)
      val method = tuple._2.getMethod("build")
      method.invoke(tuple._1)
    } else {
      throw new UnsupportedOperationException("Spark version not supported")
    }
  }

  def createObject(className: String, conArgs: Object*): (Any, Class[_]) = {
    val clazz = Utils.classForName(className)
    val ctor = clazz.getConstructors.head
    ctor.setAccessible(true)
    (ctor.newInstance(conArgs: _*), clazz)
  }

  def createObject(className: String): (Any, Class[_]) = {
    val clazz = Utils.classForName(className)
    val ctor = clazz.getConstructors.head
    ctor.setAccessible(true)
    (ctor.newInstance(), clazz)
  }
}
