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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.dict.{NBucketDictionary, NGlobalDictionaryV2}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.udf.{SplitPartImpl, TimestampAddImpl, TimestampDiffImpl, TruncateImpl}

case class DictEncode(left: Expression, mid: Expression, right: Expression) extends TernaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, StringType, StringType)

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {
    val globalDictClass = classOf[NGlobalDictionaryV2].getName
    val bucketDictClass = classOf[NBucketDictionary].getName

    val globalDictTerm = ctx.addMutableState(globalDictClass, "globalDict")
    val bucketDictTerm = ctx.addMutableState(bucketDictClass, "bucketDict")

    val dictParamsTerm = mid.simpleString
    val bucketSizeTerm = right.simpleString.toInt

    val initBucketDictFuncName = ctx.addNewFunction("initBucketDict",
      s"""
         | private void initBucketDict(int idx) {
         |   try {
         |     int bucketId = idx % $bucketSizeTerm;
         |     $globalDictTerm = new org.apache.spark.dict.NGlobalDictionaryV2("$dictParamsTerm");
         |     $bucketDictTerm = $globalDictTerm.loadBucketDictionary(bucketId);
         |   } catch (Exception e) {
         |     throw new RuntimeException(e);
         |   }
         | }
        """.stripMargin)

    ctx.addPartitionInitializationStatement(s"$initBucketDictFuncName(partitionIndex);");

    defineCodeGen(ctx, ev, (arg1, arg2, arg3) => {
      s"""$bucketDictTerm.encode($arg1)"""
    })
  }

  override def eval(input: InternalRow): Any = {
    if (input != null) {
      super.eval(input)
    } else {
      0L
    }
  }

  override def dataType: DataType = LongType

  override def children: Seq[Expression] = Seq(left, mid, right)

  override def prettyName: String = "DICTENCODE"
}

case class TimestampDiff(left: Expression, mid: Expression, right: Expression) extends TernaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringType, TypeCollection(DateType, TimestampType), TypeCollection(DateType, TimestampType))


  override protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    (mid.dataType, right.dataType) match {
      case (DateType, DateType) => TimestampDiffImpl.evaluate(input1.toString, input2.asInstanceOf[Int], input3.asInstanceOf[Int])
      case (DateType, TimestampType) => TimestampDiffImpl.evaluate(input1.toString, input2.asInstanceOf[Int], input3.asInstanceOf[Long])
      case (TimestampType, DateType) => TimestampDiffImpl.evaluate(input1.toString, input2.asInstanceOf[Long], input3.asInstanceOf[Int])
      case (TimestampType, TimestampType) =>
        TimestampDiffImpl.evaluate(input1.toString, input2.asInstanceOf[Long], input3.asInstanceOf[Long])
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val td = TimestampDiffImpl.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (arg1, arg2, arg3) => {
      s"""$td.evaluate($arg1.toString(), $arg2, $arg3)"""
    })
  }

  override def dataType: DataType = LongType

  override def children: Seq[Expression] = Seq(left, mid, right)
}

case class Truncate(_left: Expression, _right: Expression) extends BinaryExpression with ExpectsInputTypes {
  override def left: Expression = _left

  override def right: Expression = _right

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegerType, LongType, DoubleType, DecimalType, IntegerType), IntegerType)


  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    val value2 = input2.asInstanceOf[Int]
    left.dataType match {
      case IntegerType => TruncateImpl.evaluate(input1.asInstanceOf[Int], value2)
      case DoubleType => TruncateImpl.evaluate(input1.asInstanceOf[Double], value2)
      case LongType => TruncateImpl.evaluate(input1.asInstanceOf[Long], value2)
      case DecimalType() => TruncateImpl.evaluate(input1.asInstanceOf[Decimal], value2)
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tr = TruncateImpl.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (arg1, arg2) => {
      s"""$tr.evaluate($arg1, $arg2)"""
    })
  }

  override def dataType: DataType = left.dataType
}

case class TimestampAdd(left: Expression, mid: Expression, right: Expression) extends TernaryExpression with ExpectsInputTypes {

  override def dataType: DataType = right.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringType, TypeCollection(IntegerType, LongType), TypeCollection(DateType, TimestampType))


  override protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    (mid.dataType, right.dataType) match {
      case (IntegerType, DateType) => TimestampAddImpl.evaluate(input1.toString, input2.asInstanceOf[Int], input3.asInstanceOf[Int])
      case (LongType, DateType) =>
        TimestampAddImpl.evaluate(input1.toString, input2.asInstanceOf[Long], input3.asInstanceOf[Int])
      case (IntegerType, TimestampType) => TimestampAddImpl.evaluate(input1.toString, input2.asInstanceOf[Int], input3.asInstanceOf[Long])
      case (LongType, TimestampType) =>
        TimestampAddImpl.evaluate(input1.toString, input2.asInstanceOf[Long], input3.asInstanceOf[Long])
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val ta = TimestampAddImpl.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (arg1, arg2, arg3) => {
      s"""$ta.evaluate($arg1.toString(), $arg2, $arg3)"""
    })
  }

  override def children: Seq[Expression] = Seq(left, mid, right)
}

case class SplitPart(left: Expression, mid: Expression, right: Expression) extends TernaryExpression with ExpectsInputTypes {

  override def dataType: DataType = left.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, IntegerType)

  override protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    SplitPartImpl.evaluate(input1.toString, input2.toString, input3.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val ta = SplitPartImpl.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (arg1, arg2, arg3) => {
      s"""$ta.evaluate($arg1.toString(), $arg2.toString(), $arg3)"""
    })
  }

  override def children: Seq[Expression] = Seq(left, mid, right)
}