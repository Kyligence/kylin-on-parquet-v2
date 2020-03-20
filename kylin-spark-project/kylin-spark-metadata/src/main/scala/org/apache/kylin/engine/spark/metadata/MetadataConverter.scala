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

package org.apache.kylin.engine.spark.metadata


import java.{lang, util}
import java.util.regex.Pattern

import org.apache.commons.lang.StringUtils
import org.apache.kylin.common.util.DateFormat
import org.apache.kylin.cube.{CubeInstance, CubeSegment, CubeUpdate}
import org.apache.kylin.cube.cuboid.Cuboid
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity
import org.apache.kylin.engine.spark.metadata.cube.BitUtils
import org.apache.spark.sql.utils.SparkTypeUtil

import scala.collection.JavaConverters._
import org.apache.kylin.metadata.datatype.{DataType => KyDataType}
import org.apache.kylin.metadata.model.{JoinTableDesc, TableRef, TblColRef}

import scala.collection.mutable

object MetadataConverter {
  def getSegmentInfo(cubeInstance: CubeInstance, segmentId: String): SegmentInfo = {
    val allColumnDesc = extractAllColumnDesc(cubeInstance)
    val (layoutEntities, measure) = extractEntityAndMeasures(cubeInstance)
    val dictColumn = measure.values.filter(_.returnType.dataType.equals("bitmap"))
      .map(_.pra.head).toSet
    SegmentInfo(segmentId, cubeInstance.getProject, cubeInstance.getConfig, extractFactTable(cubeInstance),
      extractLookupTable(cubeInstance), extractLookupTable(cubeInstance),
      extractJoinTable(cubeInstance), allColumnDesc.asScala.values.toList, layoutEntities, mutable.Set[LayoutEntity](layoutEntities: _*),
      dictColumn,
      dictColumn,
      extractPartitionExp(cubeInstance.getSegmentById(segmentId)),
      extractFilterCondition(cubeInstance.getSegmentById(segmentId)))
  }

  def getCubeUpdate(segmentInfo: SegmentInfo): CubeUpdate = {
    null
  }

  def extractFactTable(cubeInstance: CubeInstance): TableDesc = {
    toTableDesc(cubeInstance.getModel.getRootFactTable)
  }

  def extractLookupTable(cubeInstance: CubeInstance): List[TableDesc] = {
    cubeInstance.getModel.getJoinTables
      .filter(_.getKind.equals(org.apache.kylin.metadata.model.DataModelDesc.TableKind.LOOKUP))
      .map(join => toTableDesc(join.getTableRef))
      .toList
  }

  def extractJoinTable(cubeInstance: CubeInstance): Array[JoinDesc] = {
    cubeInstance.getModel.getAllTables
      .asScala
      .map(tb => toTableDesc(tb))
    val table = cubeInstance.getModel.getJoinsTree.getTableChains
      .asScala.keys.toArray // must to be order
      .filter(!_.equals(cubeInstance.getModel.getRootFactTable.getAlias))
    val tableMap = cubeInstance.getModel.getJoinTables
      .map(join => (join.getAlias, toJoinDesc(join)))
      .toMap
    table.map(tableMap.apply)
  }

  def toJoinDesc(joinTableDesc: JoinTableDesc): JoinDesc = {
    val desc = toTableDesc(joinTableDesc.getTableRef)
    val PKS = joinTableDesc.getJoin.getPrimaryKeyColumns.map(col => toColumnDesc(ref = col))
    val FKS = joinTableDesc.getJoin.getForeignKeyColumns.map(col => toColumnDesc(ref = col))
    JoinDesc(desc, PKS, FKS, joinTableDesc.getJoin.getType)
  }

  def toTableDesc(tb: TableRef): TableDesc = {
    TableDesc(tb.getTableName, tb.getTableDesc.getDatabase,
      tb.getColumns.asScala.map(ref => toColumnDesc(ref = ref)).toList, tb.getAlias, tb.getTableDesc.getSourceType)
  }

  def extractAllColumnDesc(cubeInstance: CubeInstance): java.util.LinkedHashMap[Integer, ColumnDesc] = {
    val dimensionIndex = new util.LinkedHashMap[Integer, ColumnDesc]()
    val columns = cubeInstance.getDescriptor
      .getRowkey
      .getRowKeyColumns
    val dimensionMapping = columns
      .map(co => (co.getColRef, co.getBitIndex))
    val set = dimensionMapping.map(_._1).toSet
    val refs = cubeInstance.getAllColumns.asScala.diff(set)
      .zipWithIndex
      .map(tp => (tp._1, tp._2 + dimensionMapping.length))

    val columnIDTuples = dimensionMapping ++ refs
    val colToIndex = columnIDTuples.toMap
    columnIDTuples
      .foreach { co =>
        dimensionIndex.put(co._2, toColumnDesc(co._1, co._2))
      }
    dimensionIndex
  }

  def toLayoutEntity(cubeInstance: CubeInstance, cuboid: Cuboid): LayoutEntity = {
    val (columnIndexes, idToColumnMap, measureId) = genIDToColumnMap(cubeInstance)
    genLayoutEntity(columnIndexes, idToColumnMap, measureId, cuboid.getId)
  }

  def extractEntityAndMeasures(cubeInstance: CubeInstance): (List[LayoutEntity], Map[Integer, FunctionDesc]) = {
    val (columnIndexes, idToColumnMap, measureId) = genIDToColumnMap(cubeInstance)
    (cubeInstance.getDescriptor.getInitialCuboidScheduler
      .getAllCuboidIds
      .asScala
      .map { long =>
        genLayoutEntity(columnIndexes, idToColumnMap, measureId, long)
      }.toList, measureId.asScala.toMap)
  }

  private def genLayoutEntity(
    columnIndexes: List[Integer], idToColumnMap: java.util.Map[Integer, ColumnDesc],
    measureId: java.util.Map[Integer, FunctionDesc], long: lang.Long) = {
    val dimension = BitUtils.tailor(columnIndexes.asJava, long)
    val integerToDesc = new util.LinkedHashMap[Integer, ColumnDesc]()
    dimension.asScala.foreach(index => integerToDesc.put(index, idToColumnMap.get(index)))
    val entity = new LayoutEntity()
    entity.setId(long)
    entity.setOrderedDimensions(integerToDesc)
    entity.setOrderedMeasures(measureId)
    entity
  }

  private def genIDToColumnMap(cubeInstance: CubeInstance): (List[Integer], java.util.Map[Integer, ColumnDesc], java.util.Map[Integer, FunctionDesc]) = {
    val dimensionIndex = new util.LinkedHashMap[Integer, ColumnDesc]()
    val columns = cubeInstance.getDescriptor
      .getRowkey
      .getRowKeyColumns
    val dimensionMapping = columns
      .map(co => (co.getColRef, co.getBitIndex))
    val set = dimensionMapping.map(_._1).toSet
    val refs = cubeInstance.getAllColumns.asScala.diff(set)
      .zipWithIndex
      .map(tp => (tp._1, tp._2 + dimensionMapping.length))

    val columnIDTuples = dimensionMapping ++ refs
    val colToIndex = columnIDTuples.toMap
    columnIDTuples
      .foreach { co =>
        dimensionIndex.put(co._2, toColumnDesc(co._1, co._2))
      }
    val idToColumnMap = dimensionMapping.map(tp => Integer.valueOf(tp._2)).toList
    val measureIndex = new util.LinkedHashMap[Integer, FunctionDesc]()
    cubeInstance
      .getMeasures
      .asScala
      .zipWithIndex
      .foreach { case (measure, in) =>
        val index = in + dimensionIndex.size()
        val parameter = measure.getFunction.getParameter
        val dataType = measure.getFunction.getReturnDataType
        val parametrs = parameter.getType match {
          case "column" =>
            parameter.getColRefs.asScala
              .map(col => dimensionIndex.get(colToIndex.apply(col)))
              .toList
          case "constant" =>
            List(LiteralColumnDesc(null, SparkTypeUtil.toSparkType(dataType), null, null, -1, parameter.getValue))
        }

        val desc = FunctionDesc(measure.getName, DTType(dataType.getName, dataType.getPrecision, dataType.getScale),
          parametrs, measure.getFunction.getExpression)
        measureIndex.put(Integer.valueOf(index), desc)
      }
    (idToColumnMap, dimensionIndex, measureIndex)
  }

  def extractEntityList2JavaList(cubeInstance: CubeInstance): java.util.List[LayoutEntity] = {
    extractEntityAndMeasures(cubeInstance)._1.asJava
  }

  private def toColumnDesc(ref: TblColRef, index: Int = -1) = {
    val dataType = SparkTypeUtil.toSparkType(KyDataType.getType(ref.getDatatype))
    val columnDesc = if (ref.getColumnDesc.isComputedColumn) {
      ComputedColumnDesc(ref.getName, dataType, ref.getTableRef.getTableName, ref.getTableRef.getAlias,
        index, ref.getExpressionInSourceDB)
    } else {
      ColumnDesc(ref.getName, dataType, ref.getTableRef.getTableName, ref.getTableRef.getAlias, index)
    }
    columnDesc
  }

  def extractPartitionExp(cubeSegment: CubeSegment): String = {
    if (cubeSegment.getTSRange.startValue == 0 && cubeSegment.getTSRange.endValue == Long.MaxValue) {
      ""
    } else {
      val partitionDesc = cubeSegment.getModel.getPartitionDesc
      partitionDesc.setPartitionDateFormat(DateFormat.COMPACT_DATE_PATTERN)
      val (originPartitionColumn, convertedPartitionColumn) = if (partitionDesc.getPartitionDateColumnRef != null) {
        (partitionDesc.getPartitionDateColumnRef.getIdentity, convertFromDot(partitionDesc.getPartitionDateColumnRef.getIdentity))
      } else {
        (partitionDesc.getPartitionTimeColumnRef.getIdentity, convertFromDot(partitionDesc.getPartitionTimeColumnRef.getIdentity))
      }
      val originString = partitionDesc.getPartitionConditionBuilder
        .buildDateRangeCondition(partitionDesc, null, cubeSegment.getSegRange, null)
      StringUtils.replace(originString, originPartitionColumn, convertedPartitionColumn)
    }
  }

  def extractFilterCondition(cubeSegment: CubeSegment): String = {
    val filterCondition = cubeSegment.getModel.getFilterCondition
    if (filterCondition == null) {
      ""
    } else {
      convertFromDot(cubeSegment.getModel.getFilterCondition)
    }
  }

  private val DOT_PATTERN = Pattern.compile("(\\S+)\\.(\\D+)")

  val SEPARATOR = "_0_DOT_0_"

  def convertFromDot(withDot: String): String = {
    var m = DOT_PATTERN.matcher(withDot)
    var withoutDot = withDot
    while (m.find) {
      withoutDot = m.replaceAll("$1" + SEPARATOR + "$2")
      m = DOT_PATTERN.matcher(withoutDot)
    }
    withoutDot
  }
}
