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
package io.kyligence.kap.engine.spark.builder

import java.util.Set

import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory
import io.kyligence.kap.metadata.cube.model.{NDataSegment, NDataflow, NDataflowManager}
import org.apache.commons.lang3.RandomStringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.TaskContext
import org.apache.spark.dict.{NGlobalDictMetaInfo, NGlobalDictionaryV2}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.junit.Assert

import scala.collection.mutable

class TestGlobalDictBuild extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

  private val DEFAULT_PROJECT = "default"
  private val CUBE_NAME = "89af4ee2-2cdb-4b07-b39e-4c29856309aa"

  def getTestConfig: KylinConfig = {
    val config = KylinConfig.getInstanceFromEnv
    config
  }

  test("global dict build and checkout bucket resize strategy") {
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    Assert.assertTrue(getTestConfig.getHdfsWorkingDirectory.startsWith("file:"))
    val df: NDataflow = dsMgr.getDataflow(CUBE_NAME)
    val seg = df.getLastSegment
    val nSpanningTree = NSpanningTreeFactory.fromLayouts(seg.getIndexPlan.getAllLayouts, df.getUuid)
    val dictColSet = DictionaryBuilderHelper.extractTreeRelatedGlobalDicts(seg, nSpanningTree)
    seg.getConfig.setProperty("kylin.dictionary.globalV2-threshold-bucket-size", "100")

    // When to resize the dictionary, please refer to the description of DictionaryBuilderHelper.calculateBucketSize

    // First build dictionary, no dictionary file exists
    var randomDataSet = generateOriginData(1000, 21)
    val meta1 = buildDict(seg, randomDataSet, dictColSet)
    Assert.assertEquals(20, meta1.getBucketSize)
    Assert.assertEquals(1000, meta1.getDictCount)

    // apply rule #1
    randomDataSet = generateOriginData(3000, 22)
    val meta2 = buildDict(seg, randomDataSet, dictColSet)
    Assert.assertEquals(60, meta2.getBucketSize)
    Assert.assertEquals(4000, meta2.getDictCount)

    randomDataSet = generateOriginData(3000, 23)
    val meta3 = buildDict(seg, randomDataSet, dictColSet)
    Assert.assertEquals(60, meta3.getBucketSize)
    Assert.assertEquals(7000, meta3.getDictCount)

    // apply rule #2
    randomDataSet = generateOriginData(200, 24)
    val meta4 = buildDict(seg, randomDataSet, dictColSet)
    Assert.assertEquals(140, meta4.getBucketSize)
    Assert.assertEquals(7200, meta4.getDictCount)

    // apply rule #3
    randomDataSet = generateHotOriginData(200, 140)
    val meta5 = buildDict(seg, randomDataSet, dictColSet)
    Assert.assertEquals(140, meta5.getBucketSize)
    Assert.assertEquals(7400, meta5.getDictCount)

    // apply rule #3
    randomDataSet = generateOriginData(200, 25)
    val meta6 = buildDict(seg, randomDataSet, dictColSet)
    Assert.assertEquals(280, meta6.getBucketSize)
    Assert.assertEquals(7600, meta6.getDictCount)

    randomDataSet = generateOriginData(2000, 26)
    val meta7 = buildDict(seg, randomDataSet, dictColSet)
    Assert.assertEquals(280, meta7.getBucketSize)
    Assert.assertEquals(9600, meta7.getDictCount)
  }

  def buildDict(seg: NDataSegment, randomDataSet: Dataset[Row], dictColSet: Set[TblColRef]): NGlobalDictMetaInfo = {
    val dictionaryBuilder = new DFDictionaryBuilder(randomDataSet, seg, randomDataSet.sparkSession, dictColSet)
    val col = dictColSet.iterator().next()
    val ds = randomDataSet.select("26").distinct()
    val bucketPartitionSize = DictionaryBuilderHelper.calculateBucketSize(seg, col, ds)
    dictionaryBuilder.build(col, bucketPartitionSize, ds)
    val dict = new NGlobalDictionaryV2(seg.getProject, col.getTable, col.getName,
      seg.getConfig.getHdfsWorkingDirectory)
    dict.getMetaInfo
  }

  def generateOriginData(count: Int, length: Int): Dataset[Row] = {
    var schema = new StructType

    schema = schema.add("26", StringType)
    var set = new mutable.LinkedHashSet[Row]
    while (set.size != count) {
      val objects = new Array[String](1)
      objects(0) = RandomStringUtils.randomAlphabetic(length)
      set.+=(Row.fromSeq(objects.toSeq))
    }

    spark.createDataFrame(spark.sparkContext.parallelize(set.toSeq), schema)
  }

  def generateHotOriginData(threshold: Int, bucketSize: Int): Dataset[Row] = {
    var schema = new StructType
    schema = schema.add("26", StringType)
    var ds = generateOriginData(threshold * bucketSize * 2, 30)
    ds = ds.repartition(bucketSize, col("26"))
      .mapPartitions {
        iter =>
          val partitionID = TaskContext.get().partitionId()
          if (partitionID != 1) {
            Iterator.empty
          } else {
            iter
          }
      }(RowEncoder.apply(ds.schema))
    ds.limit(threshold)
  }
}
