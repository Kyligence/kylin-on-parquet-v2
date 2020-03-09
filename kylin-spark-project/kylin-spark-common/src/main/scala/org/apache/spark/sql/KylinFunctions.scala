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
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.KapDateTimeUtils
import org.apache.spark.sql.types.{AbstractDataType, DataType, DateType, IntegerType}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, DictEncode, Expression, ExpressionInfo, ExpressionUtils, ImplicitCastInputTypes, In, Like, Literal, RoundBase, SplitPart, Sum0, TimestampAdd, TimestampDiff, Truncate, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.udaf.{ApproxCountDistinct, IntersectCount, PreciseCountDistinct}

object KylinFunctions {
  private def withAggregateFunction(
    func: AggregateFunction,
    isDistinct: Boolean = false): Column = {
    Column(func.toAggregateExpression(isDistinct))
  }

  def dict_encode(column: Column, dictParams: Column, bucketSize: Column): Column = {
    Column(DictEncode(column.expr, dictParams.expr, bucketSize.expr))
  }

  // special lit for KE.
  def k_lit(literal: Any): Column = literal match {
    case c: Column => c
    case s: Symbol => new ColumnName(s.name)
    case _ => Column(Literal(literal))
  }

  def k_like(left: Column, right: Column): Column = Column(Like(left.expr, right.expr))

  def in(value: Expression, list: Seq[Expression]): Column = Column(In(value, list))

  def kap_day_of_week(date: Column): Column = Column(KapDayOfWeek(date.expr))

  def kap_truncate(column: Column, scale: Int): Column = {
    Column(TRUNCATE(column.expr, Literal(scale)))
  }

  def kap_subtract_months(date0: Column, date1: Column): Column = {
    Column(KapSubtractMonths(date0.expr, date1.expr))
  }

  def precise_count_distinct(column: Column): Column =
    Column(PreciseCountDistinct(column.expr).toAggregateExpression())

  def approx_count_distinct(column: Column, precision: Int): Column =
    Column(ApproxCountDistinct(column.expr, precision).toAggregateExpression())

  def intersect_count(columns: Column*): Column = {
    require(columns.size == 3, s"Input columns size ${columns.size} don't equal to 3.")
    val expressions = columns.map(_.expr)
    Column(IntersectCount(expressions.apply(0), expressions.apply(1), expressions.apply(2))
      .toAggregateExpression())
  }

  def sum0(e: Column): Column = withAggregateFunction {
    Sum0(e.expr)
  }

  val builtin: Seq[FunctionEntity] = Seq(
    FunctionEntity(ExpressionUtils.expression[TimestampAdd]("TIMESTAMPADD")),
    FunctionEntity(ExpressionUtils.expression[TimestampDiff]("TIMESTAMPDIFF")),
    FunctionEntity(ExpressionUtils.expression[Truncate]("TRUNCATE")),
    FunctionEntity(ExpressionUtils.expression[DictEncode]("DICTENCODE")),
    FunctionEntity(ExpressionUtils.expression[SplitPart]("split_part")))
}

case class FunctionEntity(
  name: FunctionIdentifier,
  info: ExpressionInfo,
  builder: FunctionBuilder)

case class TRUNCATE(child: Expression, scale: Expression)
  extends RoundBase(child, scale, BigDecimal.RoundingMode.DOWN, "DOWN")
    with Serializable with ImplicitCastInputTypes {
  def this(child: Expression) = this(child, Literal(0))
}

// scalastyle:on line.size.limit
case class KapSubtractMonths(a: Expression, b: Expression)
  extends BinaryExpression
    with ImplicitCastInputTypes {

  override def left: Expression = a

  override def right: Expression = b

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, DateType)

  override def dataType: DataType = IntegerType

  override def nullSafeEval(date0: Any, date1: Any): Any = {
    KapDateTimeUtils.dateSubtractMonths(date0.asInstanceOf[Int],
      date1.asInstanceOf[Int])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = KapDateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (d0, d1) => {
      s"""$dtu.dateSubtractMonths($d0, $d1)"""
    })
  }

  override def prettyName: String = "kap_months_between"
}

case class KapDayOfWeek(a: Expression)
  extends UnaryExpression
    with ImplicitCastInputTypes {

  override def child: Expression = a

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override protected def doGenCode(
    ctx: CodegenContext,
    ev: ExprCode): ExprCode = {
    val dtu = KapDateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (d) => {
      s"""$dtu.dayOfWeek($d)"""
    })
  }

  override def nullSafeEval(date: Any): Any = {
    KapDateTimeUtils.dayOfWeek(date.asInstanceOf[Int])
  }

  override def dataType: DataType = IntegerType

  override def prettyName: String = "kap_day_of_week"
}

object FunctionEntity {
  def apply(tuple: (String, (ExpressionInfo, FunctionBuilder))): FunctionEntity = {
    new FunctionEntity(FunctionIdentifier.apply(tuple._1), tuple._2._1, tuple._2._2)
  }
}
