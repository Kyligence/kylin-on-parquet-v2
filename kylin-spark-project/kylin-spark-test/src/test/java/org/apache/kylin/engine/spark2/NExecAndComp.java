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
package org.apache.kylin.engine.spark2;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark2.utils.QueryUtil;
import org.apache.kylin.engine.spark2.utils.RecAndQueryCompareUtil.CompareEntity;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.KylinSparkEnv;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.common.SparkQueryTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class NExecAndComp {
    private static final Logger logger = LoggerFactory.getLogger(NExecAndComp.class);

    public enum CompareLevel {
        SAME, // exec and compare
        SAME_ORDER, // exec and compare order
        SAME_ROWCOUNT, SUBSET, NONE, // batch execute
        SAME_SQL_COMPARE
    }

    static void execLimitAndValidate(List<Pair<String, String>> queries, String prj, String joinType) {
        execLimitAndValidateNew(queries, prj, joinType, null);
    }

    public static void execLimitAndValidateNew(List<Pair<String, String>> queries, String prj, String joinType,
                                               Map<String, CompareEntity> recAndQueryResult) {

        int appendLimitQueries = 0;
        for (Pair<String, String> query : queries) {
            logger.info("execLimitAndValidate on query: " + query.getFirst());
            String sql = changeJoinType(query.getSecond(), joinType);

            Pair<String, String> sqlAndAddedLimitSql = Pair.newPair(sql, sql);
            if (!sql.toLowerCase(Locale.ROOT).contains("limit ")) {
                sqlAndAddedLimitSql.setSecond(sql + " limit 5");
                appendLimitQueries++;
            }

            Dataset<Row> kapResult = (recAndQueryResult == null) ? queryWithKap(prj, joinType, sqlAndAddedLimitSql)
                    : queryWithKap(prj, joinType, sqlAndAddedLimitSql, recAndQueryResult);
            addQueryPath(recAndQueryResult, query, sql);
            Dataset<Row> sparkResult = queryWithSpark(prj, sql, query.getFirst());
            List<Row> kapRows = SparkQueryTest.castDataType(kapResult, sparkResult).toJavaRDD().collect();
            List<Row> sparkRows = sparkResult.toJavaRDD().collect();
            if (!compareResults(normRows(sparkRows), normRows(kapRows), CompareLevel.SUBSET)) {
                throw new IllegalArgumentException("Result not match");
            }
        }
        logger.info("Queries appended with limit: " + appendLimitQueries);
    }

    public static void execAndCompare(List<Pair<String, String>> queries, String prj, CompareLevel compareLevel,
                                      String joinType) {
        execAndCompareNew(queries, prj, compareLevel, joinType, null);
    }

    public static void execAndCompareQueryList(List<String> queries, String prj, CompareLevel compareLevel,
                                               String joinType) {
        List<Pair<String, String>> transformed = queries.stream().map(q -> Pair.newPair("", q))
                .collect(Collectors.toList());
        execAndCompareNew(transformed, prj, compareLevel, joinType, null);
    }

    public static void execAndCompareNew(List<Pair<String, String>> queries, String prj, CompareLevel compareLevel,
                                         String joinType, Map<String, CompareEntity> recAndQueryResult) {
        for (Pair<String, String> query : queries) {
            logger.info("Exec and compare query ({}) :{}", joinType, query.getFirst());

            String sql = changeJoinType(query.getSecond(), joinType);

            // Query from Cube
            long startTime = System.currentTimeMillis();
            Dataset<Row> cubeResult = (recAndQueryResult == null) ? queryWithKap(prj, joinType, Pair.newPair(sql, sql))
                    : queryWithKap(prj, joinType, Pair.newPair(sql, sql), recAndQueryResult);
            addQueryPath(recAndQueryResult, query, sql);
            if (compareLevel == CompareLevel.SAME) {
                Dataset<Row> sparkResult = queryWithSpark(prj, sql, query.getFirst());
                String result = SparkQueryTest.checkAnswer(SparkQueryTest.castDataType(cubeResult, sparkResult), sparkResult, false);
                if (result != null) {
                    logger.error("Failed on compare query ({}) :{}", joinType, query);
                    logger.error(result);
                    throw new IllegalArgumentException("query (" + joinType + ") :" + query + " result not match");
                }
            } else if (compareLevel == CompareLevel.NONE) {
                Dataset<Row> sparkResult = queryWithSpark(prj, sql, query.getFirst());
                List<Row> sparkRows = sparkResult.toJavaRDD().collect();
                List<Row> kapRows = SparkQueryTest.castDataType(cubeResult, sparkResult).toJavaRDD().collect();
                if (!compareResults(normRows(sparkRows), normRows(kapRows), compareLevel)) {
                    logger.error("Failed on compare query ({}) :{}", joinType, query);
                    throw new IllegalArgumentException("query (" + joinType + ") :" + query + " result not match");
                }
            } else {
                cubeResult.persist();
                System.out.println(
                        "result comparision is not available, part of the cube results: " + cubeResult.count());
                cubeResult.show();
                cubeResult.unpersist();
            }
            logger.info("The query ({}) : {} cost {} (ms)", joinType, query, System.currentTimeMillis() - startTime);
        }
    }

    public static boolean execAndCompareQueryResult(Pair<String, String> queryForKap,
                                                    Pair<String, String> queryForSpark, String joinType, String prj,
                                                    Map<String, CompareEntity> recAndQueryResult) {
        String sqlForSpark = changeJoinType(queryForSpark.getSecond(), joinType);
        addQueryPath(recAndQueryResult, queryForSpark, sqlForSpark);
        Dataset<Row> sparkResult = queryWithSpark(prj, queryForSpark.getSecond(), queryForSpark.getFirst());
        List<Row> sparkRows = sparkResult.toJavaRDD().collect();

        String sqlForKap = changeJoinType(queryForKap.getSecond(), joinType);
        Dataset<Row> cubeResult = queryWithKap(prj, joinType, Pair.newPair(sqlForKap, sqlForKap));
        List<Row> kapRows = SparkQueryTest.castDataType(cubeResult, sparkResult).toJavaRDD().collect();

        return sparkRows.equals(kapRows);
    }

    private static List<Row> normRows(List<Row> rows) {
        List<Row> rowList = Lists.newArrayList();
        rows.forEach(row -> {
            rowList.add(SparkQueryTest.prepareRow(row));
        });
        return rowList;
    }

    private static void addQueryPath(Map<String, CompareEntity> recAndQueryResult, Pair<String, String> query,
                                     String modifiedSql) {
        if (recAndQueryResult == null) {
            return;
        }

        Preconditions.checkState(recAndQueryResult.containsKey(modifiedSql));
        recAndQueryResult.get(modifiedSql).setFilePath(query.getFirst());
    }

    @Deprecated
    static void execCompareQueryAndCompare(List<Pair<String, String>> queries, String prj, String joinType) {
        throw new IllegalStateException(
                "The method has deprecated, please call org.apache.kylin.engine.spark2.NExecAndComp.execAndCompareNew");
    }

    private static Dataset<Row> queryWithKap(String prj, String joinType, Pair<String, String> pair,
                                             Map<String, CompareEntity> compareEntityMap) {

        compareEntityMap.putIfAbsent(pair.getFirst(), new CompareEntity());
        final CompareEntity entity = compareEntityMap.get(pair.getFirst());
        entity.setSql(pair.getFirst());
        Dataset<Row> rowDataset = queryFromCube(prj, changeJoinType(pair.getSecond(), joinType));
        entity.setOlapContexts(OLAPContext.getThreadLocalContexts());
        OLAPContext.clearThreadLocalContexts();
        return rowDataset;
    }

    private static Dataset<Row> queryWithKap(String prj, String joinType, Pair<String, String> sql) {
        return queryFromCube(prj, changeJoinType(sql.getSecond(), joinType));
    }

    private static Dataset<Row> queryWithSpark(String prj, String originSql, String sqlPath) {
        String compareSql = getCompareSql(sqlPath);
        if (StringUtils.isEmpty(compareSql))
            compareSql = originSql;

        String afterConvert = QueryUtil.massagePushDownSql(compareSql, prj, "default", false);
        // Table schema comes from csv and DATABASE.TABLE is not supported.
        String sqlForSpark = removeDataBaseInSql(afterConvert);
        return querySparkSql(sqlForSpark);
    }

    public static String removeDataBaseInSql(String originSql) {
        return originSql.replaceAll("(?i)edw\\.", "") //
                .replaceAll("`edw`\\.", "") //
                .replaceAll("\"EDW\"\\.", "") //
                .replaceAll("`EDW`\\.", "") //
                .replaceAll("(?i)default\\.", "") //
                .replaceAll("`default`\\.", "") //
                .replaceAll("\"DEFAULT\"\\.", "") //
                .replaceAll("`DEFAULT`\\.", "") //
                .replaceAll("(?i)TPCH\\.", "") //
                .replaceAll("`TPCH`\\.", "") //
                .replaceAll("`tpch`\\.", "") //
                .replaceAll("(?i)TDVT\\.", "") //
                .replaceAll("\"TDVT\"\\.", "") //
                .replaceAll("`TDVT`\\.", "") //
                .replaceAll("\"POPHEALTH_ANALYTICS\"\\.", "") //
                .replaceAll("`POPHEALTH_ANALYTICS`\\.", "") //
                .replaceAll("(?i)ISSUES\\.", "");
    }

    public static List<Pair<String, String>> fetchQueries(String folder) throws IOException {
        File sqlFolder = new File(folder);
        return retrieveITSqls(sqlFolder);
    }

    public static List<Pair<String, String>> fetchPartialQueries(String folder, int start, int end) throws IOException {
        File sqlFolder = new File(folder);
        List<Pair<String, String>> originalSqls = retrieveITSqls(sqlFolder);
        if (end > originalSqls.size()) {
            end = originalSqls.size();
        }
        return originalSqls.subList(start, end);
    }

    @SuppressWarnings("unused")
    private static List<Pair<String, String>> retrieveAllQueries(String baseDir) throws IOException {
        File[] sqlFiles = new File[0];
        if (baseDir != null) {
            File sqlDirF = new File(baseDir);
            if (sqlDirF.exists() && sqlDirF.listFiles() != null) {
                sqlFiles = new File(baseDir).listFiles((dir, name) -> name.startsWith("sql_"));
            }
        }
        List<Pair<String, String>> allSqls = new ArrayList<>();
        for (File file : Objects.requireNonNull(sqlFiles)) {
            allSqls.addAll(retrieveITSqls(file));
        }
        return allSqls;
    }

    private static List<Pair<String, String>> retrieveITSqls(File file) throws IOException {
        File[] sqlFiles = new File[0];
        if (file != null && file.exists() && file.listFiles() != null) {
            sqlFiles = file.listFiles((dir, name) -> name.endsWith(".sql"));
        }
        List<Pair<String, String>> ret = Lists.newArrayList();
        assert sqlFiles != null;
        Arrays.sort(sqlFiles, (o1, o2) -> {
            final String idxStr1 = o1.getName().replaceAll("\\D", "");
            final String idxStr2 = o2.getName().replaceAll("\\D", "");
            if (idxStr1.isEmpty() || idxStr2.isEmpty()) {
                return String.CASE_INSENSITIVE_ORDER.compare(o1.getName(), o2.getName());
            }
            return Integer.parseInt(idxStr1) - Integer.parseInt(idxStr2);
        });
        for (File sqlFile : sqlFiles) {
            String sqlStatement = FileUtils.readFileToString(sqlFile, "UTF-8").trim();
            int semicolonIndex = sqlStatement.lastIndexOf(";");
            String sql = semicolonIndex == sqlStatement.length() - 1 ? sqlStatement.substring(0, semicolonIndex)
                    : sqlStatement;
            ret.add(Pair.newPair(sqlFile.getCanonicalPath(), sql + '\n'));
        }
        return ret;
    }

    public static boolean compareResults(List<Row> expectedResult, List<Row> actualResult, CompareLevel compareLevel) {
        boolean good = true;
        if (compareLevel == CompareLevel.SAME_ORDER) {
            good = expectedResult.equals(actualResult);
        }
        if (compareLevel == CompareLevel.SAME) {
            if (expectedResult.size() == actualResult.size()) {
                if (expectedResult.size() > 15000) {
                    throw new RuntimeException(
                            "please modify the sql to control the result size that less than 15000 and it has "
                                    + actualResult.size() + " rows");
                }
                for (Row eRow : expectedResult) {
                    if (!actualResult.contains(eRow)) {
                        good = false;
                        break;
                    }
                }
            } else {
                good = false;
            }
        }

        if (compareLevel == CompareLevel.SAME_ROWCOUNT) {
            long count1 = expectedResult.size();
            long count2 = actualResult.size();
            good = count1 == count2;
        }

        if (compareLevel == CompareLevel.SUBSET) {
            for (Row eRow : actualResult) {
                if (!expectedResult.contains(eRow)) {
                    good = false;
                    break;
                }
            }
        }

        if (!good) {
            logger.error("Result not match");
            printRows("expected", expectedResult);
            printRows("actual", actualResult);
        }
        return good;
    }

    private static void printRows(String source, List<Row> rows) {
        System.out.println("***********" + source + " start**********");
        rows.forEach(row -> System.out.println(row.mkString(" | ")));
        System.out.println("***********" + source + " end**********");
    }

    private static void compareResults(Dataset<Row> expectedResult, Dataset<Row> actualResult,
                                       CompareLevel compareLevel) {
        Preconditions.checkArgument(expectedResult != null);
        Preconditions.checkArgument(actualResult != null);

        try {
            expectedResult.persist();
            actualResult.persist();

            boolean good = true;

            if (compareLevel == CompareLevel.SAME) {
                long count1 = expectedResult.except(actualResult).count();
                long count2 = actualResult.except(expectedResult).count();
                if (count1 != 0 || count2 != 0) {
                    good = false;
                }
            }

            if (compareLevel == CompareLevel.SAME_ROWCOUNT) {
                long count1 = expectedResult.count();
                long count2 = actualResult.count();
                good = count1 == count2;
            }

            if (compareLevel == CompareLevel.SUBSET) {
                long count1 = actualResult.except(expectedResult).count();
                good = count1 == 0;
            }

            if (!good) {
                logger.error("Result not match");
                expectedResult.show(10000);
                actualResult.show(10000);
                throw new IllegalStateException();
            }
        } finally {
            expectedResult.unpersist();
            actualResult.unpersist();
        }
    }

    public static List<Pair<String, String>> doFilter(List<Pair<String, String>> sources,
                                                      final Set<String> exclusionList) {
        Preconditions.checkArgument(sources != null);
        Set<String> excludes = Sets.newHashSet(exclusionList);
        return sources.stream().filter(pair -> {
            final String[] splits = pair.getFirst().split(File.separator);
            return !excludes.contains(splits[splits.length - 1]);
        }).collect(Collectors.toList());
    }

    public static Dataset<Row> queryFromCube(String prj, String sqlText) {
        sqlText = QueryUtil.massageSql(sqlText, prj, 0, 0, "DEFAULT", true);
        return sql(prj, sqlText, null);
    }

    public static Dataset<Row> querySparkSql(String sqlText) {
        logger.info("Fallback this sql to original engine...");
        long startTs = System.currentTimeMillis();
        Dataset<Row> r = KylinSparkEnv.getSparkSession().sql(sqlText);
        logger.info("Duration(ms): {}", (System.currentTimeMillis() - startTs));
        return r;
    }

    public static Dataset<Row> sql(String prj, String sqlText) {
        return sql(prj, sqlText, null);
    }

    public static Dataset<Row> sql(String prj, String sqlText, List<String> parameters) {
        if (sqlText == null)
            throw new RuntimeException("Sorry your SQL is null...");

        try {
            logger.info("Try to query from cube....");
            long startTs = System.currentTimeMillis();
            Dataset<Row> dataset = queryCubeAndSkipCompute(prj, sqlText, parameters);
            logger.info("Cool! This sql hits cube...");
            logger.info("Duration(ms): {}", (System.currentTimeMillis() - startTs));
            return dataset;
        } catch (Throwable e) {
            logger.error("There is no cube can be used for query [{}]", sqlText);
            logger.error("Reasons:", e);
            throw new RuntimeException("Error in running query [ " + sqlText.trim() + " ]", e);
        }
    }

    static Dataset<Row> queryCubeAndSkipCompute(String prj, String sql, List<String> parameters) throws Exception {
        KylinSparkEnv.skipCompute();
        Dataset<Row> df = queryCube(prj, sql, parameters);
        return df;
    }

    static Dataset<Row> queryCubeAndSkipCompute(String prj, String sql) throws Exception {
        KylinSparkEnv.skipCompute();
        Dataset<Row> df = queryCube(prj, sql, null);
        return df;
    }

    public static Dataset<Row> queryCube(String prj, String sql) throws SQLException {
        return queryCube(prj, sql, null);
    }

    public static Dataset<Row> queryCube(String prj, String sql, List<String> parameters) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = QueryConnection.getConnection(prj);
            stmt = conn.prepareStatement(sql);
            for (int i = 1; parameters != null && i <= parameters.size(); i++) {
                stmt.setString(i, parameters.get(i - 1).trim());
            }
            rs = stmt.executeQuery();
        } finally {
            DBUtils.closeQuietly(rs);
            DBUtils.closeQuietly(stmt);
            DBUtils.closeQuietly(conn);
            //KylinSparkEnv.cleanCompute();
        }
        return (Dataset<Row>) QueryContextFacade.current().getDataset();
    }

    private static String getCompareSql(String originSqlPath) {
        if (!originSqlPath.endsWith(".sql")) {
            return "";
        }
        File file = new File(originSqlPath + ".expected");
        if (!file.exists())
            return "";

        try {
            return FileUtils.readFileToString(file, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("meet error when reading compared spark sql from {}", file.getAbsolutePath());
            return "";
        }
    }

    public static List<List<String>> queryCubeWithJDBC(String prj, String sql) throws Exception {
        //      KylinSparkEnv.skipCompute();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        List<List<String>> results = Lists.newArrayList();
        try {
            conn = QueryConnection.getConnection(prj);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<String> oneRow = Lists.newArrayListWithCapacity(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    oneRow.add((rs.getString(i + 1)));
                }

                results.add(oneRow);
            }
        } finally {
            DBUtils.closeQuietly(rs);
            DBUtils.closeQuietly(stmt);
            DBUtils.closeQuietly(conn);
        }
        return results;
    }

    public static String changeJoinType(String sql, String targetType) {

        if (targetType.equalsIgnoreCase("default")) {
            return sql;
        }

        String specialStr = "changeJoinType_DELIMITERS";
        sql = sql.replaceAll(System.getProperty("line.separator"), " " + specialStr + " ");

        String[] tokens = StringUtils.split(sql, null);// split white spaces
        for (int i = 0; i < tokens.length - 1; ++i) {
            if ((tokens[i].equalsIgnoreCase("inner") || tokens[i].equalsIgnoreCase("left"))
                    && tokens[i + 1].equalsIgnoreCase("join")) {
                tokens[i] = targetType.toLowerCase(Locale.ROOT);
            }
        }

        String ret = StringUtils.join(tokens, " ");
        ret = ret.replaceAll(specialStr, System.getProperty("line.separator"));
        logger.info("The actual sql executed is: " + ret);

        return ret;
    }
}
