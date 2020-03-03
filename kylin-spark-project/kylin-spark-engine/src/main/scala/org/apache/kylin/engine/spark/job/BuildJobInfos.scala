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

package org.apache.kylin.engine.spark.job

import java.util

import org.apache.kylin.engine.spark.metadata.cube.model.{LayoutEntity, SpanningTree}
import org.apache.kylin.engine.spark.metadata.SegmentInfo
import org.apache.spark.application.RetryInfo
import org.apache.spark.sql.execution.SparkPlan

class BuildJobInfos {
  // BUILD
  private val seg2cuboidsNumPerLayer: util.Map[String, util.List[Int]] = new util.HashMap[String, util.List[Int]]

  private val seg2SpanningTree: java.util.Map[String, SpanningTree] = new util.HashMap[String, SpanningTree]

  private val parent2Children: util.Map[LayoutEntity, util.List[Long]] = new util.HashMap[LayoutEntity, util.List[Long]]

  // MERGE
  private val sparkPlans: java.util.List[SparkPlan] = new util.LinkedList[SparkPlan]

  private val mergingSegments: java.util.List[SegmentInfo] = new util.LinkedList[SegmentInfo]

  // COMMON
  private val abnormalLayouts: util.Map[Long, util.List[String]] = new util.HashMap[Long, util.List[String]]

  private var retryTimes = 0

  private val autoSparkConfs: java.util.Map[String, String] = new util.HashMap[String, String]

  private val jobRetryInfos: java.util.List[RetryInfo] = new util.LinkedList[RetryInfo]

  var buildTime: Long = 0L

  private var jobStartTime: Long = 0L

  var waitTime: Long = 0L

  private var waitStartTime: Long = 0L

  def startJob(): Unit = {
    jobStartTime = System.currentTimeMillis()
  }
  def jobEnd(): Unit = {
    buildTime = System.currentTimeMillis() - jobStartTime
  }

  def startWait(): Unit = {
    waitStartTime = System.currentTimeMillis()
  }

  def endWait(): Unit = {
    waitTime = System.currentTimeMillis() - waitStartTime
  }

  def recordSpanningTree(segId: String, tree: SpanningTree): Unit = {
    seg2SpanningTree.put(segId, tree)
  }

  def getSpanningTree(segId: String): SpanningTree = {
    seg2SpanningTree.get(segId)
  }

  def recordMergingSegments(segments: util.List[SegmentInfo]): Unit = {
    mergingSegments.addAll(segments)
  }

  def clearMergingSegments(): Unit = {
    mergingSegments.clear()
  }

  def getMergingSegments: util.List[SegmentInfo] = {
    mergingSegments
  }

  def recordSparkPlan(plan: SparkPlan): Unit = {
    sparkPlans.add(plan)
  }

  def clearSparkPlans(): Unit = {
    sparkPlans.clear()
  }

  def getSparkPlans: util.List[SparkPlan] = {
    sparkPlans
  }

  def getAbnormalLayouts: util.Map[Long, util.List[String]] = {
    abnormalLayouts
  }

  def recordAbnormalLayouts(key: Long, value: String): Unit = {
    if (abnormalLayouts.containsKey(key)) {
      abnormalLayouts.get(key).add(value)
    } else {
      val reasons = new util.LinkedList[String]()
      reasons.add(value)
      abnormalLayouts.put(key, reasons)
    }
  }

  def getAutoSparkConfs: util.Map[String, String] = {
    autoSparkConfs
  }

  def recordAutoSparkConfs(confs: util.Map[String, String]): Unit = {
    autoSparkConfs.putAll(confs)
  }

  def getJobRetryInfos: util.List[RetryInfo] = {
    jobRetryInfos
  }

  def recordJobRetryInfos(info: RetryInfo): Unit = {
    jobRetryInfos.add(info)
  }

  def recordRetryTimes(times: Int): Unit = {
    retryTimes = times
  }

  def getRetryTimes: Int = {
    retryTimes
  }

  def recordCuboidsNumPerLayer(segId: String, num: Int): Unit = {
    if (seg2cuboidsNumPerLayer.containsKey(segId)) {
      seg2cuboidsNumPerLayer.get(segId).add(num)
    } else {
      val nums = new util.LinkedList[Int]()
      nums.add(num)
      seg2cuboidsNumPerLayer.put(segId, nums)
    }
  }

  def clearCuboidsNumPerLayer(segId: String): Unit = {
    if (seg2cuboidsNumPerLayer.containsKey(segId)) {
      seg2cuboidsNumPerLayer.get(segId).clear()
    }
  }

  def getSeg2cuboidsNumPerLayer: util.Map[String, util.List[Int]] = {
    seg2cuboidsNumPerLayer
  }

  def recordParent2Children(key: LayoutEntity, value: util.List[Long]): Unit = {
    parent2Children.put(key, value)
  }

  def getParent2Children: util.Map[LayoutEntity, util.List[Long]] = {
    parent2Children
  }
}
