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

package org.apache.kylin.query

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import org.apache.kylin.metadata.datatype.DataType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{FunctionEntity, KylinFunctions, SparkSession}
import org.apache.spark.sql.catalyst.expressions.SparderAggFun

class UdfManager(sparkSession: SparkSession) extends Logging {
  private var udfCache: Cache[String, String] = _

  KylinFunctions.builtin.foreach { case FunctionEntity(name, info, builder) =>
    sparkSession.sessionState.functionRegistry.registerFunction(name, info, builder)
  }
  udfCache = CacheBuilder.newBuilder
    .maximumSize(100)
    .expireAfterWrite(1, TimeUnit.HOURS)
    .removalListener(new RemovalListener[String, String]() {
      override def onRemoval(notification: RemovalNotification[String, String]): Unit = {
        val func = notification.getKey
        logInfo(s"remove function $func")
      }
    })
    .build
    .asInstanceOf[Cache[String, String]]

  def destory(): Unit = {
    udfCache.cleanUp()
  }

  def doRegister(dataType: DataType, funcName: String): String = {
    val name = genKey(dataType, funcName)
    val cacheFunc = udfCache.getIfPresent(name)
    if (cacheFunc == null) {
      sparkSession.udf.register(name, new SparderAggFun(funcName, dataType))
      udfCache.put(name, "")
    }
    name
  }

  def genKey(dataType: DataType, funcName: String): String = {
    dataType.toString
      .replace("(", "_")
      .replace(")", "_")
      .replace(",", "_") + funcName
  }

}

object UdfManager {

  private val defaultManager = new AtomicReference[UdfManager]
  private val defaultSparkSession: AtomicReference[SparkSession] =
    new AtomicReference[SparkSession]

  def create(sparkSession: SparkSession): Unit = {
    val manager = new UdfManager(sparkSession)
    defaultManager.set(manager)
    defaultSparkSession.set(sparkSession)
  }

  def register(dataType: DataType, func: String): String = {
    defaultManager.get().doRegister(dataType, func)
  }

}
