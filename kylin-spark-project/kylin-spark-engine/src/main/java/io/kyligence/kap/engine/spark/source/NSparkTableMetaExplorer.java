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
package io.kyligence.kap.engine.spark.source;

import java.io.Serializable;
import java.net.URI;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import scala.Option;

public class NSparkTableMetaExplorer implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkTableMetaExplorer.class);

    enum PROVIDER {
        HIVE("hive"), UNSPECIFIED("");

        private static final PROVIDER[] ALL = new PROVIDER[] { HIVE };
        private String value;

        PROVIDER(String value) {
            this.value = value;
        }

        public static PROVIDER fromString(Option<String> value) {
            if (value.isEmpty()) {
                return UNSPECIFIED;
            }

            for (PROVIDER provider : ALL) {
                if (provider.value.equals(value.get())) {
                    return provider;
                }
            }
            return UNSPECIFIED;
        }
    }

    private static final List<String> UNSUPOORT_TYPE = Lists.newArrayList("array", "map", "struct");

    private static final Map<PROVIDER, String> PROVIDER_METADATA_TYPE_STRING = new EnumMap<>(PROVIDER.class);

    static {
        PROVIDER_METADATA_TYPE_STRING.put(PROVIDER.HIVE, "HIVE_TYPE_STRING");
    }

    public NSparkTableMeta getSparkTableMeta(String database, String tableName) {
        SessionCatalog catalog = SparderEnv.getSparkSession().sessionState().catalog();
        TableIdentifier tableIdentifier = TableIdentifier.apply(tableName,
                Option.apply(database.isEmpty() ? null : database));
        CatalogTable tableMetadata = catalog.getTempViewOrPermanentTableMetadata(tableIdentifier);
        checkTableIsValid(tableMetadata, tableIdentifier, database, tableName);
        return getSparkTableMeta(tableName, tableMetadata);
    }

    private NSparkTableMeta getSparkTableMeta(String tableName, CatalogTable tableMetadata) {
        NSparkTableMetaBuilder builder = new NSparkTableMetaBuilder();
        builder.setTableName(tableName);
        builder.setAllColumns(getAllColumns(tableMetadata));
        builder.setOwner(tableMetadata.owner());
        builder.setCreateTime(tableMetadata.createTime() + "");
        builder.setLastAccessTime(tableMetadata.lastAccessTime() + "");
        builder.setTableType(tableMetadata.tableType().name());

        if (tableMetadata.storage().inputFormat().isDefined()) {
            builder.setSdInputFormat(tableMetadata.storage().inputFormat().get());
        }
        if (tableMetadata.storage().outputFormat().isDefined()) {
            builder.setSdOutputFormat(tableMetadata.storage().outputFormat().get());
        }
        Option<URI> uriOption = tableMetadata.storage().locationUri();
        if (uriOption.isDefined()) {
            builder.setSdLocation(uriOption.get().toString());
        }
        if (tableMetadata.provider().isDefined()) {
            builder.setProvider(tableMetadata.provider().get());
        }
        if (tableMetadata.properties().contains("totalSize")) {
            builder.setFileSize(Long.parseLong(tableMetadata.properties().get("totalSize").get()));
        }
        if (tableMetadata.properties().contains("numFiles")) {
            builder.setFileNum(Long.parseLong(tableMetadata.properties().get("numFiles").get()));
        }
        return builder.createSparkTableMeta();
    }

    private List<NSparkTableMeta.SparkTableColumnMeta> getAllColumns(CatalogTable tableMetadata) {
        List<NSparkTableMeta.SparkTableColumnMeta> allColumns = Lists
                .newArrayListWithCapacity(tableMetadata.schema().size());
        for (org.apache.spark.sql.types.StructField field : tableMetadata.schema().fields()) {
            String type = field.dataType().simpleString();

            // fetch provider specified type
            PROVIDER provider = PROVIDER.fromString(tableMetadata.provider());
            if (provider != PROVIDER.UNSPECIFIED
                    && field.metadata().contains(PROVIDER_METADATA_TYPE_STRING.get(provider))) {
                type = field.metadata().getString(PROVIDER_METADATA_TYPE_STRING.get(provider));
            }
            allColumns.add(new NSparkTableMeta.SparkTableColumnMeta(field.name(), type,
                    field.getComment().isDefined() ? field.getComment().get() : null));
        }

        return allColumns;
    }

    private void checkTableIsValid(CatalogTable tableMetadata, TableIdentifier tableIdentifier, String database,
            String tableName) {
        for (NSparkTableMeta.SparkTableColumnMeta colMeta : getAllColumns(tableMetadata)) {
            String type = colMeta.dataType;
            if (UNSUPOORT_TYPE.stream().filter(t -> type.contains(t)).findAny().isPresent()) {
                throw new RuntimeException("Error for parser table: " + tableName + ", filed: " + colMeta.name
                        + " ,unsupoort type: " + type);
            }
        }

        if (CatalogTableType.VIEW().equals(tableMetadata.tableType())) {
            try {
                SparderEnv.getSparkSession().table(tableIdentifier).queryExecution().analyzed();
            } catch (Throwable e) {
                logger.error("Error for parser view: " + tableName, e);
                throw new RuntimeException("Error for parser view: " + tableName + ", " + e.getMessage()
                        + "(There are maybe syntactic differences between HIVE and SparkSQL)", e);
            }
        }
        if (tableMetadata.properties().contains("skip.header.line.count")) {
            throw new RuntimeException(
                    "The current product version does not support such source data tables, which are generally converted from a CSV table with a header. Please change the table to a table without a header.");
        }
    }
}
