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

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.collection.mutable

class ColumnDesc(val columnName: String, val dataType: DataType, val tableName: String, val tableAliasName: String, val id: Int) {
  def identity: String = s"$tableAliasName.$columnName"

  def isColumnType: Boolean = true
}
object ColumnDesc{
  def apply(columnName: String, dataType: DataType, tableName: String, tableAliasName: String, id: Int):
  ColumnDesc = new ColumnDesc(columnName, dataType, tableName, tableAliasName, id)
}

case class LiteralColumnDesc(
  override val columnName: String, override val dataType: DataType,
  override val tableName: String, override val tableAliasName: String, override val id: Int, val value: Any)
  extends ColumnDesc(columnName, dataType, tableName, tableAliasName, id) {
  override def isColumnType: Boolean = false

}

case class ComputedColumnDesc(
  override val columnName: String, override val dataType: DataType,
  override val tableName: String, override val tableAliasName: String, override val id: Int, val expression: String = "")
  extends ColumnDesc(columnName, dataType, tableName, tableAliasName, id)

case class TableDesc(tableName: String, databaseName: String, columns: List[ColumnDesc], alias: String, sourceType: Int) {
  def identity: String = s"$databaseName.$tableName"

  def toSchema: StructType = {
    StructType(columns.map(column => StructField(column.columnName, column.dataType)))
  }
}

case class FunctionDesc(functionName: String, returnType: DTType, pra: List[ColumnDesc], expression: String)

case class DTType(dataType: String, precision: Int) {
  def toKylinDataType: org.apache.kylin.metadata.datatype.DataType = {
    org.apache.kylin.metadata.datatype.DataType.getType(s"$dataType($precision)")
  }
}

case class JoinDesc(lookupTable: TableDesc, PKS: Array[ColumnDesc], FKS: Array[ColumnDesc], joinType: String)

case class SegmentInfo(id: String,
  project: String,
  kylinconf: KylinConfig,
  factTable: TableDesc,
  lookupTables: List[TableDesc],
  snapshotTables: List[TableDesc],
  joindescs: Array[JoinDesc],
  allColumns: List[ColumnDesc],
  layouts: List[LayoutEntity],
  var toBuildLayouts: mutable.Set[LayoutEntity],
  var toBuildDictColumns: Set[ColumnDesc],
  allDictColumns: Set[ColumnDesc],
  partitionExp: String,
  filterCondition: String) {

  def updateLayout(layoutEntity: LayoutEntity): Unit = {
    toBuildLayouts.remove(layoutEntity)
  }
}