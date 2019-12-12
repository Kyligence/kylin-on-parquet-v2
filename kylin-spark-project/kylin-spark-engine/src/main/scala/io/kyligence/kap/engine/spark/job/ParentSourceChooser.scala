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

package io.kyligence.kap.engine.spark.job

import com.google.common.base.Preconditions
import com.google.common.collect.{Lists, Maps}
import io.kyligence.kap.engine.spark.builder._
import io.kyligence.kap.engine.spark.utils.SparkDataSource._
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.metadata.cube.model.DataModel.TableKind
import org.apache.kylin.engine.spark.metadata.cube.model.{CubeJoinedFlatTableDesc, CuboidLayoutChooser, DataModel, DataSegment, IndexEntity, LayoutEntity, SpanningTree}
import org.apache.kylin.engine.spark.metadata.cube.MetadataConverter
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import scala.collection.JavaConverters._

class ParentSourceChooser(toBuildTree: SpanningTree,
                          var seg: DataSegment,
                          jobId: String,
                          ss: SparkSession,
                          config: KylinConfig,
                          needEncoding: Boolean) extends Logging {

  // build from built cuboid.
  var reuseSources: java.util.Map[java.lang.Long, NBuildSourceInfo] = Maps.newHashMap()

  // build from flattable.
  var flatTableSource: NBuildSourceInfo = _
  val flatTableDesc =
    new CubeJoinedFlatTableDesc(seg, DFChooser.needJoinLookupTables(seg.getModel, toBuildTree))

  val flatTableDesc = new CubeJoinedFlatTableDesc(
    MetadataConverter.getCubeDesc(seg.getCube),
    ParentSourceChooser.needJoinLookupTables(seg.getModel, toBuildTree))

  def decideSources(): Unit = {
    toBuildTree.getRootIndexEntities.asScala.foreach { entity =>
      val parentLayout = CuboidLayoutChooser.selectLayoutForBuild(seg, entity)
      if (parentLayout != null) {
        decideParentLayoutSource(entity, parentLayout)
      } else {
        decideFlatTableSource(entity)
      }
    }
  }

  private def decideFlatTableSource(entity: IndexEntity): Unit = {
    if (flatTableSource == null) {
      if (needEncoding) {
        // hacked, for some case, you do not want to trigger buildSnapshot
        // eg: resource detect
        // Move this to a more suitable place
        val builder = new DFSnapshotBuilder(seg, ss)
        seg = builder.buildSnapshot
      }
      flatTableSource = getFlatTable()
    }
    flatTableSource.addCuboid(entity)
  }

  private def decideParentLayoutSource(entity: IndexEntity, layout: LayoutEntity): Unit = {
    val id = layout.getId
    if (reuseSources.containsKey(id)) {
      reuseSources.get(id).addCuboid(entity)
    } else {
      val source = getSourceFromLayout(layout, entity)
      reuseSources.put(id, source)
    }
  }

  def persistFlatTableIfNecessary(): String = {
    var path = ""
    if (flatTableSource != null && flatTableSource.getToBuildCuboids.size() > config.getPersistFlatTableThreshold) {

      val df = flatTableSource.getFlattableDS
      if (df.schema.nonEmpty) {
        val allUsedCols = flatTableSource.getToBuildCuboids.asScala.flatMap { c =>
          val dims = c.getDimensions.asScala.map(_.toString)

          val measureUsedCols = c.getEffectiveMeasures.asScala.flatMap { mea =>
            val parameters = mea._2.getFunction.getParameters.asScala
            if (parameters.head.isColumnType) {
              val colIndex = df.schema.fieldNames.zipWithIndex.map(tp => (tp._2, tp._1)).toMap
              parameters.map(p => colIndex.apply(flatTableDesc.getColumnIndex(p.getColRef)).toString)
            } else {
              Nil
            }
          }

          dims ++ measureUsedCols
        }.toSeq

        df.select(allUsedCols.map(col): _*)
        path = s"${config.getJobTmpFlatTableDir(seg.getProject, jobId)}"
        ss.sparkContext.setJobDescription("Persist flat table.")
        df.write.mode(SaveMode.Overwrite).parquet(path)
        logInfo(s"Persist flat table into:$path. Selected cols in table are $allUsedCols.")
        flatTableSource.setParentStoragePath(path)
      }
    }
    path
  }

  private def persistFactViewIfNecessary(): String = {
    var path = ""
    if (needEncoding) {
      logInfo(s"Check project:${seg.getProject} seg:${seg.getName} persist view fact table.")
      val fact = flatTableDesc.getDataModel.getRootFactTable
      val globalDicts = DictionaryBuilderHelper.extractTreeRelatedGlobalDicts(seg, toBuildTree)
      val existsFactDictCol = globalDicts.asScala.exists(_.getTableRef.getTableIdentity.equals(fact.getTableIdentity))

      if (fact.getTableDesc.isView && existsFactDictCol) {
        val viewDS = ss.table(fact.getTableDesc).alias(fact.getAlias)
        path = s"${config.getJobTmpViewFactTableDir(seg.getProject, jobId)}"
        ss.sparkContext.setJobDescription("Persist view fact table.")
        viewDS.write.mode(SaveMode.Overwrite).parquet(path)
        logInfo(s"Persist view fact table into:$path.")
      }
    }
    path
  }

  private def getSourceFromLayout(layout: LayoutEntity,
                                  indexEntity: IndexEntity): NBuildSourceInfo = {
    val buildSource = new NBuildSourceInfo
    val segDetails = seg.getSegDetails
    val dataCuboid = segDetails.getLayoutById(layout.getId)
    Preconditions.checkState(dataCuboid != null)
    buildSource.setParentStoragePath(NSparkCubingUtil.getStoragePath(dataCuboid))
    buildSource.setSparkSession(ss)
    buildSource.setCount(dataCuboid.getRows)
    buildSource.setLayoutId(layout.getId)
    buildSource.setByteSize(dataCuboid.getByteSize)
    buildSource.addCuboid(indexEntity)
    logInfo(s"Reuse a suitable layout: ${layout.getId} for building cuboid: ${indexEntity.getId}")
    buildSource
  }

  private def getFlatTable(): NBuildSourceInfo = {
    val viewPath = persistFactViewIfNecessary()
    val sourceInfo = new NBuildSourceInfo
    sourceInfo.setSparkSession(ss)
    sourceInfo.setLayoutId(ParentSourceChooser.FLAT_TABLE_FLAG)
    sourceInfo.setViewFactTablePath(viewPath)

    val needJoin = ParentSourceChooser.needJoinLookupTables(seg.getModel, toBuildTree)
    val flatTableDesc = new CubeJoinedFlatTableDesc(seg.getCube, seg.getSegRange, needJoin)
    val flatTable = new CreateFlatTable(flatTableDesc, seg, toBuildTree, ss, sourceInfo)
    val afterJoin: Dataset[Row] = flatTable.generateDataset(needEncoding, needJoin)
    sourceInfo.setFlattableDS(afterJoin)

    logInfo("No suitable ready layouts could be reused, generate dataset from flat table.")
    sourceInfo
  }
}

object ParentSourceChooser {
  def apply(toBuildTree: SpanningTree,
            seg: DataSegment,
            jobId: String,
            ss: SparkSession,
            config: KylinConfig,
            needEncoding: Boolean): ParentSourceChooser =
    new ParentSourceChooser(toBuildTree: SpanningTree,
      seg: DataSegment,
      jobId,
      ss: SparkSession,
      config: KylinConfig,
      needEncoding)

  val FLAT_TABLE_FLAG: Long = -1L

  def needJoinLookupTables(model: DataModel, toBuildTree: SpanningTree): Boolean = {
    val conf = KylinConfig.getInstanceFromEnv
    if (!conf.isFlatTableJoinWithoutLookup) {
      return true
    }

    val joinTables = model.getJoinTables
    val factTable = model.getRootFactTable
    var needJoin = false
    if (joinTables.asScala.count(_.getJoin.isLeftJoin) != joinTables.size()) {
      needJoin = true
    }

    if (joinTables.asScala.count(_.getKind == TableKind.LOOKUP) != joinTables.size()) {
      needJoin = true
    }

    val toBuiltCols: Set[TblColRef] = toBuildTree.getRootIndexEntities.asScala.flatMap(index => {
      val measureUsedCols = index.getEffectiveMeasures.asScala.flatMap(_._2.getFunction.getColRefs.asScala)
      val dimUsedCols = index.getEffectiveDimCols.asScala.values
      measureUsedCols ++ dimUsedCols
    }).toSet

    toBuiltCols.foreach(col =>
      if (!factTable.getTableIdentity.equalsIgnoreCase(col.getTableRef.getTableIdentity)) {
        needJoin = true
      }
    )

    needJoin
  }
}
