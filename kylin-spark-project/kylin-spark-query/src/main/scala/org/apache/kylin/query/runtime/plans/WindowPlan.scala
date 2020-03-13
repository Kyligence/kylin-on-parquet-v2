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
package org.apache.kylin.query.runtime.plans

import java.sql.Date
import java.util.{Calendar, Locale}

import org.apache.calcite.DataContext
import org.apache.calcite.rel.RelCollationImpl
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.util.NlsString
import org.apache.kylin.query.relnode.{OLAPProjectRel, OLAPWindowRel}
import org.apache.kylin.query.runtime.SparderRexVisitor
import org.apache.spark.sql.KylinFunctions._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.utils.SparkTypeUtil

import scala.collection.JavaConverters._

object WindowPlan extends Logging {
  // the function must have sort
  val sortSpecified =
    List("CUME_DIST", "LEAD", "RANK", "DENSE_RANK", "ROW_NUMBER", "NTILE", "LAG")

  // the function must row range
  val rowSpecified =
    List("RANK", "PERCENT_RANK", "DENSE_RANK", "NTILE", "ROW_NUMBER")

  // the function no range
  val nonRangeSpecified = List(
    "LAG",
    "LEAD"
  )

  def window(
    input: java.util.List[DataFrame],
    rel: OLAPWindowRel, datacontex: DataContext): DataFrame = {
    val start = System.currentTimeMillis()

    var windowCount = 0
    rel.groups.asScala.head.upperBound
    val df = input.get(0)
    val columnSize = df.schema.length

    val columns = df.schema.fieldNames.map(col)
    val constantMap = rel.getConstants.asScala
      .map(_.getValue)
      .zipWithIndex
      .map { entry =>
        (entry._2 + columnSize, entry._1)
      }.toMap[Int, Any]
    val visitor = new SparderRexVisitor(df,
      rel.getInput.getRowType,
      datacontex)
    val constants = rel.getConstants.asScala
      .map { constant =>
        k_lit(Literal.apply(constant.accept(visitor)))
      }
    val columnsAndConstants = columns ++ constants
    val windows = rel.groups.asScala
      .flatMap { group =>
        var isDateTimeFamilyType = false
        val fieldsNameToType = rel.getInput.getRowType.getFieldList.asScala.zipWithIndex
          .map {
            case (field, index) => index -> field.getType.getSqlTypeName.toString
          }.toMap

        fieldsNameToType.foreach(map =>
          if (SparkTypeUtil.isDateTimeFamilyType(map._2)) {
            isDateTimeFamilyType = true
          })

        var orderByColumns = group.orderKeys
          .asInstanceOf[RelCollationImpl]
          .getFieldCollations
          .asScala
          .map { fieldIndex =>
            var column = columns.apply(fieldIndex.getFieldIndex)
            if (!group.isRows && fieldsNameToType(fieldIndex.getFieldIndex).equalsIgnoreCase("timestamp")) {
              column = column.cast(LongType)
            }
            fieldIndex.direction match {
              case Direction.DESCENDING =>
                column = column.desc
              case Direction.STRICTLY_DESCENDING =>
                column = column.desc
              case Direction.ASCENDING =>
                column = column.asc
              case Direction.STRICTLY_ASCENDING =>
                column = column.asc
              case _ =>
            }
            column
          }
          .toList
        val partitionColumns = group.keys.asScala
          .map(fieldIndex => columns.apply(fieldIndex))
          .toSeq
        group.aggCalls.asScala.map { agg =>
          var windowDesc: WindowSpec = null
          val opName = agg.op.getName.toUpperCase(Locale.ROOT)
          val numberConstants = constantMap
            .filter(_._2.isInstanceOf[Number])
            .map { entry =>
              (entry._1, entry._2.asInstanceOf[Number])
            }.toMap
          var (lowerBound: Long, upperBound: Long) = buildRange(group, numberConstants, isDateTimeFamilyType, group.isRows)
          if (orderByColumns.nonEmpty) {

            windowDesc = Window.orderBy(orderByColumns: _*)
            if (!nonRangeSpecified.contains(opName)) {
              if (group.isRows || rowSpecified.contains(opName)) {
                windowDesc = windowDesc.rowsBetween(lowerBound, upperBound)
              } else {
                windowDesc = windowDesc.rangeBetween(lowerBound, upperBound)
              }
            }
          } else {
            if (sortSpecified.contains(opName)) {
              windowDesc = Window.orderBy(k_lit(1))
              if (!nonRangeSpecified.contains(opName)) {
                if (group.isRows || rowSpecified.contains(opName)) {
                  windowDesc = windowDesc.rowsBetween(lowerBound, upperBound)
                } else {
                  windowDesc = windowDesc.rangeBetween(lowerBound, upperBound)
                }
              }
            }
          }
          if (partitionColumns.nonEmpty) {
            windowDesc =
              if (windowDesc == null) Window.partitionBy(partitionColumns: _*)
              else windowDesc.partitionBy(partitionColumns: _*)
          }

          val func = opName match {
            case "ROW_NUMBER" =>
              row_number()
            case "RANK" =>
              rank()
            case "DENSE_RANK" =>
              dense_rank()
            case "FIRST_VALUE" =>
              first(
                columnsAndConstants.apply(
                  agg.operands.asScala.head.asInstanceOf[RexInputRef].getIndex))
            case "LAST_VALUE" =>
              last(
                columnsAndConstants.apply(
                  agg.operands.asScala.head.asInstanceOf[RexInputRef].getIndex))
            case "LEAD" =>
              val args =
                agg.operands.asScala.map(_.asInstanceOf[RexInputRef].getIndex)
              args.size match {
                // offset default value is 1 in spark
                case 1 => lead(columnsAndConstants.apply(args.head), 1)
                case 2 => lead(columnsAndConstants.apply(args.head),
                  constantMap.apply(args(1)).asInstanceOf[Number].intValue())
                case 3 => {
                  lead(columnsAndConstants.apply(args.head),
                    constantMap.apply(args(1)).asInstanceOf[Number].intValue(),
                    constantValue(rel, constantMap, args(2), visitor))
                }
              }

            case "LAG" =>
              val args =
                agg.operands.asScala.map(_.asInstanceOf[RexInputRef].getIndex)
              args.size match {
                // offset default value is 1 in spark
                case 1 => lag(columnsAndConstants.apply(args.head), 1)
                case 2 => lag(columnsAndConstants.apply(args.head),
                  constantMap.apply(args(1)).asInstanceOf[Number].intValue())
                case 3 =>
                  lag(columnsAndConstants.apply(args.head),
                    constantMap.apply(args(1)).asInstanceOf[Number].intValue(),
                    constantValue(rel, constantMap, args(2), visitor))
              }
            case "NTILE" =>
              ntile(constantMap
                .apply(
                  agg.operands.asScala.head.asInstanceOf[RexInputRef].getIndex)
                .asInstanceOf[Number].intValue())
            case "COUNT" =>
              count(
                if (agg.operands.isEmpty) {
                  k_lit(1)
                } else {
                  columnsAndConstants.apply(
                    agg.operands.asScala.head.asInstanceOf[RexInputRef].getIndex)
                }
              )
            case "MAX" =>
              max(
                columnsAndConstants.apply(
                  agg.operands.asScala.head.asInstanceOf[RexInputRef].getIndex))
            case x if opName.contains("SUM") =>
              sum(
                columnsAndConstants.apply(
                  agg.operands.asScala.head.asInstanceOf[RexInputRef].getIndex))
            case "MIN" =>
              min(
                columnsAndConstants.apply(
                  agg.operands.asScala.head.asInstanceOf[RexInputRef].getIndex))
            case "AVG" =>
              avg(
                columnsAndConstants.apply(
                  agg.operands.asScala.head.asInstanceOf[RexInputRef].getIndex))
          }
          windowCount = windowCount + 1
          val alias = s"${System.identityHashCode(rel)}_window_" + windowCount
          if (windowDesc == null) {
            func.over().alias(alias)
          } else {
            func.over(windowDesc).alias(alias)
          }
        }

      }
    val selectColumn = columns ++ windows
    val window = df.select(selectColumn: _*)
    logInfo(s"Gen window cost Time :${System.currentTimeMillis() - start} ")
    window
  }

  // scalastyle:off
  def constantValue(rel: OLAPWindowRel, constantMap: Map[Int, Any], idx: Int, rexVisitor: SparderRexVisitor) = {
    if (constantMap.contains(idx)) {
      constantMap(idx) match {
        case v: NlsString => v.getValue
        case v: Calendar => new Date(v.getTimeInMillis)
        case other => other
      }
    } else {
      rel.getInput match {
        case input: OLAPProjectRel => // the constant value might be functions like CURRENT_DATE
          input.getProjects.get(idx).accept(rexVisitor) match {
            case col: Column => col.expr
            case lit: Literal => lit
            case _ =>
          }
        case _ =>
          throw new IllegalStateException("Unsupported window function format");
      }
    }
  }

  def constantValue(value: Any) = {
    value match {
      case v: NlsString => v.getValue
      case v: Calendar => new Date(v.getTimeInMillis)
      case other => other
    }
  }

  def buildRange(
    group: org.apache.calcite.rel.core.Window.Group,
    constantMap: Map[Int, Number],
    isDateType: Boolean,
    isRows: Boolean): (Long, Long) = {
    var lowerBound = Window.currentRow
    if (group.lowerBound.isPreceding) {
      if (group.lowerBound.isUnbounded) {
        lowerBound = Window.unboundedPreceding
      } else {
        lowerBound = -constantMap
          .apply(group.lowerBound.getOffset.asInstanceOf[RexInputRef].getIndex)
          .longValue()
        if (isDateType && !isRows) {
          lowerBound = lowerBound / 1000
        }

      }
    } else if (group.lowerBound.isFollowing) {
      if (group.lowerBound.isUnbounded) {
        lowerBound = Window.unboundedFollowing
      } else {
        lowerBound = constantMap
          .apply(group.lowerBound.getOffset.asInstanceOf[RexInputRef].getIndex)
          .longValue()
        if (isDateType && !isRows) {
          lowerBound = lowerBound / 1000
        }

      }
    }

    var upperBound = Window.currentRow
    if (group.upperBound.isPreceding) {
      if (group.upperBound.isUnbounded) {
        upperBound = Window.unboundedPreceding
      } else {
        upperBound = -constantMap
          .apply(group.upperBound.getOffset.asInstanceOf[RexInputRef].getIndex)
          .longValue()
        if (isDateType && !isRows) {
          upperBound = upperBound / 1000
        }

      }
    } else if (group.upperBound.isFollowing) {
      if (group.upperBound.isUnbounded) {
        upperBound = Window.unboundedFollowing
      } else {
        upperBound = constantMap
          .apply(group.upperBound.getOffset.asInstanceOf[RexInputRef].getIndex)
          .longValue()
        if (isDateType && !isRows) {
          upperBound = upperBound / 1000
        }
      }
    }

    (lowerBound, upperBound)
  }
}
