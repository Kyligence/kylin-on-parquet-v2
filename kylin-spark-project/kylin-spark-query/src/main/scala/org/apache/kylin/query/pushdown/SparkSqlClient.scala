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

package org.apache.kylin.query.pushdown

import java.util.{UUID, List => JList}

import org.apache.kylin.common.exceptions.KylinTimeoutException
import org.apache.kylin.common.util.{HadoopUtil, Pair}
import org.apache.kylin.common.{KylinConfig, QueryContextFacade}
import org.apache.kylin.engine.spark.metadata.cube.StructField
import org.apache.kylin.query.runtime.plans.QueryToExecutionIDCache
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.utils.{QueryMetricUtils, ResourceDetectUtils}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.utils.SparkTypeUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object SparkSqlClient {
	val logger: Logger = LoggerFactory.getLogger(classOf[SparkSqlClient])

	def executeSql(ss: SparkSession, sql: String, uuid: UUID): Pair[JList[JList[String]], JList[StructField]] = {
		ss.sparkContext.setLocalProperty("spark.scheduler.pool", "query_pushdown")
		HadoopUtil.setCurrentConfiguration(ss.sparkContext.hadoopConfiguration)
		val s = "Start to run sql with SparkSQL..."
		val queryId = QueryContextFacade.current().getQueryId
		ss.sparkContext.setLocalProperty(QueryToExecutionIDCache.KYLIN_QUERY_ID_KEY, queryId)
		logger.info(s)

		val df = ss.sql(sql)

		autoSetShufflePartitions(ss, df)

		val msg = "SparkSQL returned result DataFrame"
		logger.info(msg)

		DFToList(ss, sql, uuid, df)
	}

	private def autoSetShufflePartitions(ss: SparkSession, df: DataFrame) = {
		val config = KylinConfig.getInstanceFromEnv
		if (config.isAutoSetPushDownPartitions) {
			try {
				val basePartitionSize = config.getBaseShufflePartitionSize
				val paths = ResourceDetectUtils.getPaths(df.queryExecution.sparkPlan)
				val sourceTableSize = ResourceDetectUtils.getResourceSize(paths: _*) + "b"
				val partitions = Math.max(1, JavaUtils.byteStringAsMb(sourceTableSize) / basePartitionSize).toString
				df.sparkSession.sessionState.conf.setLocalProperty("spark.sql.shuffle.partitions", partitions)
				logger.info(s"Auto set spark.sql.shuffle.partitions $partitions")
			} catch {
				case e: Throwable =>
					logger.error("Auto set spark.sql.shuffle.partitions failed.", e)
			}
		}
	}

	private def DFToList(ss: SparkSession, sql: String, uuid: UUID, df: DataFrame): Pair[JList[JList[String]], JList[StructField]] = {
		val jobGroup = Thread.currentThread.getName
		ss.sparkContext.setJobGroup(jobGroup, s"Push down: $sql", interruptOnCancel = true)
		try {
			val temporarySchema = df.schema.fields.zipWithIndex.map {
				case (_, index) => s"temporary_$index"
			}
			val tempDF = df.toDF(temporarySchema: _*)
			val columns = tempDF.schema.map(tp => col(s"`${tp.name}`").cast(StringType))
			val frame = tempDF.select(columns: _*)
			val rowList = frame.collect().map(_.toSeq.map(_.asInstanceOf[String]).asJava).toSeq.asJava
			val fieldList = df.schema.map(field => SparkTypeUtil.convertSparkFieldToJavaField(field)).asJava
			val (scanRows, scanBytes) = QueryMetricUtils.collectScanMetrics(frame.queryExecution.executedPlan)
			Pair.newPair(rowList, fieldList)
		} catch {
			case e: Throwable =>
				if (e.isInstanceOf[InterruptedException]) {
					ss.sparkContext.cancelJobGroup(jobGroup)
					logger.info("Query timeout ", e)
					Thread.currentThread.interrupt()
					throw new KylinTimeoutException("Query timeout after: " + KylinConfig.getInstanceFromEnv.getQueryTimeoutSeconds + "s")
				}
				else throw e
		} finally {
			df.sparkSession.sessionState.conf.setLocalProperty("spark.sql.shuffle.partitions", null)
			HadoopUtil.setCurrentConfiguration(null)
		}
	}

}

class SparkSqlClient
