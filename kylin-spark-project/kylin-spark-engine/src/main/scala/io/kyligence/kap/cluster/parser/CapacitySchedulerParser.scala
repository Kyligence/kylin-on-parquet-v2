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

package io.kyligence.kap.cluster.parser

import java.util.{List => JList}

import com.fasterxml.jackson.databind.JsonNode
import io.kyligence.kap.cluster.{AvailableResource, ResourceInfo}

import scala.collection.JavaConverters._

class CapacitySchedulerParser extends SchedulerParser {

  override def availableResource(queueName: String): AvailableResource = {
    val queues: JList[JsonNode] = root.findParents("queueName")
    val nodes = queues.asScala.filter(queue => parseValue(queue.get("queueName")).equals(queueName))
    require(nodes.size == 1)
    val (queueAvailable, queueMax) = queueCapacity(nodes.head)
    val totalResource = calTotalResource(nodes.head)
    val clusterNode = root.findValue("schedulerInfo")
    val cluster = clusterAvailableCapacity(clusterNode)
    val min = Math.min(queueAvailable, cluster)
    val resource = AvailableResource(totalResource.percentage(min), totalResource.percentage(queueMax))
    logInfo(s"Capacity actual available resource: $resource.")
    resource
  }

  private def clusterAvailableCapacity(node: JsonNode): Double = {
    val max = parseValue(node.get("capacity")).toDouble
    val used = parseValue(node.get("usedCapacity")).toDouble
    val capacity = (max - used) / 100
    logInfo(s"Cluster available capacity: $capacity.")
    capacity
  }

  private def queueCapacity(node: JsonNode): (Double, Double) = {
    val max = parseValue(node.get("absoluteMaxCapacity")).toDouble
    val used = parseValue(node.get("absoluteUsedCapacity")).toDouble
    val available = (max - used) / 100
    logInfo(s"Queue available capacity: $available.")
    (available, max / 100)
  }

  private def calTotalResource(node: JsonNode): ResourceInfo = {
    val usedMemory = parseValue(node.get("resourcesUsed").get("memory")).toInt
    if (usedMemory != 0) {
      val usedCapacity = parseValue(node.get("absoluteUsedCapacity")).toDouble / 100
      val resource = ResourceInfo(Math.floor(usedMemory / usedCapacity).toInt, Int.MaxValue)
      logInfo(s"Estimate total cluster resource is $resource.")
      resource
    } else {
      logInfo("Current queue used memory is 0, seem available resource as infinite.")
      ResourceInfo(Int.MaxValue, Int.MaxValue)
    }
  }
}
