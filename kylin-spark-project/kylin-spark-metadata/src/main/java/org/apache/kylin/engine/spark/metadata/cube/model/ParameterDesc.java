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
import com.google.common.collect.Sets;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Objects;
import java.util.Set;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class ParameterDesc implements Serializable {

    public static ParameterDesc newInstance(Object obj) {
        ParameterDesc param = new ParameterDesc();

        if (obj instanceof TblColRef) {
            TblColRef col = (TblColRef) obj;
            param.type = FunctionDesc.PARAMETER_TYPE_COLUMN;
            param.value = col.getIdentity();
            param.colRef = col;
        } else {
            param.type = FunctionDesc.PARAMETER_TYPE_CONSTANT;
            param.value = (String) obj;
        }

        return param;
    }

    @JsonProperty("type")
    private String type;
    @JsonProperty("value")
    private String value;

    private TblColRef colRef = null;

    public byte[] getBytes() throws UnsupportedEncodingException {
        return value.getBytes("UTF-8");
    }

    public void setColRef(TblColRef colRef) {
        this.colRef = colRef;
    }

    public boolean isColumnType() {
        return FunctionDesc.PARAMETER_TYPE_COLUMN.equals(type);
    }

    public boolean isConstant() {
        return FunctionDesc.PARAMETER_TYPE_CONSTANT.equalsIgnoreCase(type);
    }

    public boolean isConstantParameterDesc() {
        TblColRef colRef = this.getColRef();
        if (colRef == null || isConstant())
            return true;
        Set<TblColRef> collector = Sets.newHashSet();
        TblColRef.collectSourceColumns(colRef, collector);
        return collector.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ParameterDesc that = (ParameterDesc) o;

        if (type != null ? !type.equals(that.type) : that.type != null)
            return false;

        ParameterDesc p = this;
        ParameterDesc q = that;

        if (p.isColumnType() != q.isColumnType()) {
            return false;
        }

        if (p.isColumnType() && !Objects.equals(q.getColRef(), p.getColRef())) {
            return false;
        }

        if (!p.isColumnType() && !p.value.equals(q.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (colRef != null ? colRef.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return isColumnType() && colRef != null ? colRef.toString() : value;
    }

    public String getType() {
        return this.type;
    }

    public String getValue() {
        return this.value;
    }

    public TblColRef getColRef() {
        return this.colRef;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
