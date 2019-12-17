package org.apache.kylin.engine.spark.metadata


import org.apache.kylin.cube.{CubeInstance, CubeUpdate}
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity
import org.apache.spark.sql.util.SparkTypeUtil

import scala.collection.JavaConverters._
import org.apache.kylin.metadata.datatype.{DataType => KyDataType}
import org.apache.kylin.metadata.model.TblColRef

object MetadataConverter {
  def getSegmentInfo(cubeInstance: CubeInstance): SegmentInfo = {
    extractEntity(cubeInstance)
    null
  }

  def getCubeUpdate(segmentInfo: SegmentInfo): CubeUpdate = {
    null
  }

  def extractEntity(cubeInstance: CubeInstance): List[LayoutEntity] = {
    val dimensionMapping = cubeInstance.getDescriptor
      .getRowkey
      .getRowKeyColumns
      .map(co => (co.getColRef, co.getBitIndex))
      .toMap
    val dimensionMap = dimensionMapping
      .map { co =>
        (co._2, toColumnDesc(co._2, co._1))
      }
      .toMap
    val values = dimensionMap.keys.toList
    val measureId = cubeInstance
      .getMeasures
      .asScala
      .zipWithIndex
      .map { case (measure, in) =>
        val index = in + dimensionMap.size
        val parametrs = measure.getFunction.getParameter
          .getColRefs.asScala
          .map(col => toColumnDesc(dimensionMapping.apply(col), col))
          .toList
        val dataType = measure.getFunction.getReturnDataType
        (Integer.valueOf(index), FunctionDesc(measure.getName, DTType(dataType.getName, dataType.getPrecision),
          parametrs, measure.getFunction.getExpression))
      } .toMap.asJava

    cubeInstance.getDescriptor.getInitialCuboidScheduler
      .getAllCuboidIds
      .asScala
      .map { long =>
        val dimension = tailor(values, long)
        val orderDimension = dimension.map(index => (index, dimensionMap.apply(index))).toMap.asJava
        val entity = new LayoutEntity()
        entity.setOrderedDimensions(orderDimension)
        entity.setOrderedMeasures(measureId)
        entity
      }.toList
  }

  private def toColumnDesc(index: Int, ref: TblColRef) = {
    val dataType = SparkTypeUtil.toSparkType(KyDataType.getType(ref.getDatatype))
    val columnDesc = if (ref.getColumnDesc.isComputedColumn) {
      ComputedColumnDesc(ref.getName, dataType, ref.getTableRef.getTableName, ref.getTableRef.getAlias, index, ref.getExpressionInSourceDB)
    } else {
      ColumnDesc(ref.getName, dataType, ref.getTableRef.getTableName, ref.getTableRef.getAlias, index)
    }
    columnDesc
  }

  private def tailor(complete: List[Int], cuboidId: Long): Array[Integer] = {
    val bitCount: Int = java.lang.Long.bitCount(cuboidId)
    val ret: Array[Integer] = new Array[Integer](bitCount)
    var next: Int = 0
    val size: Int = complete.size
    for (i <- 0 until size) {
      val shift: Int = size - i - 1
      if ((cuboidId & (1L << shift)) != 0)
        next += 1
      ret(next) = complete.apply(i)
    }
    ret
  }
}
