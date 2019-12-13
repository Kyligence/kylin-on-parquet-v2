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

package org.apache.kylin.engine.spark.metadata.cube.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class JoinDesc implements Serializable {

    // inner, left, right, outer...
    @JsonProperty("type")
    private String type;
    @JsonProperty("primary_key")
    private String[] primaryKey;
    @JsonProperty("foreign_key")
    private String[] foreignKey;
    @JsonProperty("non_equi_join_condition")
    private NonEquiJoinCondition nonEquiJoinCondition;
    @JsonProperty("primary_table")
    private String primaryTable;
    @JsonProperty("foreign_table")
    private String foreignTable;

    private TblColRef[] primaryKeyColumns;
    private TblColRef[] foreignKeyColumns;
    private TableRef primaryTableRef;
    private TableRef foreignTableRef;

    public void swapPKFK() {
        String[] t = primaryKey;
        primaryKey = foreignKey;
        foreignKey = t;

        TblColRef[] tt = primaryKeyColumns;
        primaryKeyColumns = foreignKeyColumns;
        foreignKeyColumns = tt;
    }

    public boolean isInnerJoin() {
        return "INNER".equalsIgnoreCase(type);
    }
    
    public boolean isLeftJoin() {
        return "LEFT".equalsIgnoreCase(type);
    }

    public String getPrimaryTable() {
        return primaryTable;
    }

    public String getForeignTable() {
        return foreignTable;
    }

    public void setPrimaryTableRef(TableRef primaryTableRef) {
        this.primaryTableRef = primaryTableRef;
    }

    public void setForeignTableRef(TableRef foreignTableRef) {
        this.foreignTableRef = foreignTableRef;
    }

    public NonEquiJoinCondition getNonEquiJoinCondition() {
        return nonEquiJoinCondition;
    }

    public void setNonEquiJoinCondition(NonEquiJoinCondition nonEquiJoinCondition) {
        this.nonEquiJoinCondition = nonEquiJoinCondition;
    }

    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }

    public String[] getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String[] primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String[] getForeignKey() {
        return foreignKey;
    }

    public void setForeignKey(String[] foreignKey) {
        this.foreignKey = foreignKey;
    }

    public TblColRef[] getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    public void setPrimaryKeyColumns(TblColRef[] primaryKeyColumns) {
        checkSameTable(primaryKeyColumns);
        this.primaryKeyColumns = primaryKeyColumns;
    }

    public TblColRef[] getForeignKeyColumns() {
        return foreignKeyColumns;
    }

    public void setForeignKeyColumns(TblColRef[] foreignKeyColumns) {
        checkSameTable(primaryKeyColumns);
        this.foreignKeyColumns = foreignKeyColumns;
    }

    private void checkSameTable(TblColRef[] cols) {
        if (cols == null || cols.length == 0)
            return;
        
        TableRef tableRef = cols[0].getTableRef();
        for (int i = 1; i < cols.length; i++)
            Preconditions.checkState(tableRef == cols[i].getTableRef());
    }

    public TableRef getPKSide() {
        return primaryTableRef != null ? primaryTableRef : primaryKeyColumns[0].getTableRef();
    }
    
    public TableRef getFKSide() {
        return foreignTableRef != null ? foreignTableRef : foreignKeyColumns[0].getTableRef();
    }

    public void sortByFK() {
        Preconditions.checkState(primaryKey.length == foreignKey.length && primaryKey.length == primaryKeyColumns.length && foreignKey.length == foreignKeyColumns.length);
        boolean cont = true;
        int n = foreignKey.length;
        for (int i = 0; i < n - 1 && cont; i++) {
            cont = false;
            for (int j = 0; j < n - 1 - i; j++) {
                int jj = j + 1;
                if (foreignKey[j].compareTo(foreignKey[jj]) > 0) {
                    swap(foreignKey, j, jj);
                    swap(primaryKey, j, jj);
                    swap(foreignKeyColumns, j, jj);
                    swap(primaryKeyColumns, j, jj);
                    cont = true;
                }
            }
        }
    }

    private void swap(String[] arr, int j, int jj) {
        String tmp = arr[j];
        arr[j] = arr[jj];
        arr[jj] = tmp;
    }

    private void swap(TblColRef[] arr, int j, int jj) {
        TblColRef tmp = arr[j];
        arr[j] = arr[jj];
        arr[jj] = tmp;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(primaryKeyColumns);
        result = prime * result + Arrays.hashCode(foreignKeyColumns);
        result = prime * result + this.type.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        JoinDesc other = (JoinDesc) obj;


        // note pk/fk are sorted, sortByFK()
        if (!Arrays.equals(foreignKey, other.foreignKey))
            return false;
        if (!Arrays.equals(primaryKey, other.primaryKey))
            return false;
        if (!Arrays.equals(foreignKeyColumns, other.foreignKeyColumns))
            return false;
        if (!Arrays.equals(primaryKeyColumns, other.primaryKeyColumns))
            return false;

        if (!this.type.equalsIgnoreCase(other.getType()))
            return false;
        return true;
    }

    // equals() without alias
    public boolean matches(JoinDesc other) {
        if (other == null)
            return false;

        if (!this.type.equalsIgnoreCase(other.getType()))
            return false;

        // note pk/fk are sorted, sortByFK()
        if (!this.columnDescEquals(foreignKeyColumns, other.foreignKeyColumns))
            return false;
        if (!this.columnDescEquals(primaryKeyColumns, other.primaryKeyColumns))
            return false;

        return true;
    }

    private boolean columnDescEquals(TblColRef[] a, TblColRef[] b) {
        if (a.length != b.length)
            return false;

        for (int i = 0; i < a.length; i++) {
            if (a[i].getColumnDesc().equals(b[i].getColumnDesc()) == false)
                return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "JoinDesc [type=" + type + ", primary_key=" + Arrays.toString(primaryKey) + ", foreign_key="
                + Arrays.toString(foreignKey) + "]";
    }

    public static class JoinDescBuilder {

        private List<String> pks = new ArrayList<String>();
        private List<TblColRef> pkCols = new ArrayList<TblColRef>();
        private List<String> fks = new ArrayList<String>();
        private List<TblColRef> fkCols = new ArrayList<TblColRef>();
        private TableRef primaryTableRef;
        private TableRef foreignTableRef;
        private NonEquiJoinCondition nonEquiJoinCondition;
        private String type;


        public JoinDescBuilder addPrimaryKeys(String[] pkColNames, TblColRef[] colRefs) {
            pks.addAll(Arrays.asList(pkColNames));
            pkCols.addAll(Arrays.asList(colRefs));
            return this;
        }

        public JoinDescBuilder addForeignKeys(String[] fkColNames, TblColRef[] colRefs) {
            fks.addAll(Arrays.asList(fkColNames));
            fkCols.addAll(Arrays.asList(colRefs));
            return this;
        }

        public JoinDescBuilder addPrimaryKeys(Collection<TblColRef> colRefs) {
            pks.addAll(colRefs.stream().map(TblColRef::getName).collect(Collectors.toList()));
            pkCols.addAll(colRefs);
            return this;
        }

        public JoinDescBuilder addForeignKeys(Collection<TblColRef> colRefs) {
            fks.addAll(colRefs.stream().map(TblColRef::getName).collect(Collectors.toList()));
            fkCols.addAll(colRefs);
            return this;
        }

        public JoinDescBuilder setPrimaryTableRef(TableRef primaryTableRef) {
            this.primaryTableRef = primaryTableRef;
            return this;
        }

        public JoinDescBuilder setForeignTableRef(TableRef foreignTableRef) {
            this.foreignTableRef = foreignTableRef;
            return this;
        }

        public void setNonEquiJoinCondition(NonEquiJoinCondition nonEquiJoinCondition) {
            this.nonEquiJoinCondition = nonEquiJoinCondition;
        }

        public void setType(String type) {
            this.type = type;
        }

        public JoinDesc build() {
            JoinDesc join = new JoinDesc();
            join.setForeignKey(fks.toArray(new String[0]));
            join.setForeignKeyColumns(fkCols.toArray(new TblColRef[fkCols.size()]));
            join.setPrimaryKey(pks.toArray(new String[0]));
            join.setPrimaryKeyColumns(pkCols.toArray(new TblColRef[pkCols.size()]));
            join.primaryTable = primaryTableRef.getAlias();
            join.foreignTable = foreignTableRef.getAlias();
            join.primaryTableRef = primaryTableRef;
            join.foreignTableRef = foreignTableRef;
            join.nonEquiJoinCondition = nonEquiJoinCondition;
            join.type = type;
            return join;
        }
    }

}
