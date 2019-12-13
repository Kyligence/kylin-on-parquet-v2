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

import io.kyligence.kap.engine.spark.job.NSparkCubingUtil._
import org.apache.kylin.engine.spark.metadata.cube.model.TblColRef
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, Dataset, Row}

import scala.util.{Failure, Success, Try}

object DFBuilderHelper extends Logging {

  val ENCODE_SUFFIX = "_KE_ENCODE"

  def filterCols(dsSeq: Seq[Dataset[Row]], needCheckCols: Set[TblColRef]): Set[TblColRef] = {
    needCheckCols -- dsSeq.flatMap(ds => filterCols(ds, needCheckCols))
  }

  def filterCols(ds: Dataset[Row], needCheckCols: Set[TblColRef]): Set[TblColRef] = {
    needCheckCols.filter(cc =>
      isValidExpr(convertFromDot(cc.getExpressionInSourceDB), ds))
  }

  def isValidExpr(colExpr: String, ds: Dataset[Row]): Boolean = {
    Try(ds.select(expr(colExpr))) match {
      case Success(_) =>
        true
      case Failure(_) =>
        false
    }
  }

  def chooseSuitableCols(ds: Dataset[Row], needCheckCols: Iterable[TblColRef]): Seq[Column] = {
    needCheckCols
      .filter(ref => isValidExpr(ref.getExpressionInSourceDB, ds))
      .map(ref => expr(convertFromDot(ref.getExpressionInSourceDB)).alias(convertFromDot(ref.getIdentity)))
      .toSeq
  }

  def time[R](msg: String, block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    logInfo(s"$msg. Elapsed time: ${t1 - t0} ms")
    result
  }

}
