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

package org.apache.kylin.rest;

import org.apache.catalina.Context;
import org.apache.catalina.core.AprLifecycleListener;
import org.apache.catalina.core.StandardServer;
import org.apache.catalina.deploy.ErrorPage;
import org.apache.catalina.startup.Tomcat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class DebugTomcat {

    public static void setupDebugEnv() {
        try {
            System.setProperty("HADOOP_USER_NAME", "root");
            System.setProperty("log4j.configuration", "file:../build/conf/kylin-tools-log4j.properties");

            // test_case_data/sandbox/ contains HDP 2.2 site xmls which is dev sandbox
            KylinConfig.setSandboxEnvIfPossible();
            // Must set SandboxEnv before checking the Kerberos status
            if (UserGroupInformation.isSecurityEnabled()) {
                authKrb5();
            }
            overrideDevJobJarLocations();

            System.setProperty("spring.profiles.active", "testing");

            //avoid log permission issue
            if (System.getProperty("catalina.home") == null)
                System.setProperty("catalina.home", ".");

            if (StringUtils.isEmpty(System.getProperty("hdp.version"))) {
                System.setProperty("hdp.version", "2.4.0.0-169");
            }

            // workaround for job submission from win to linux -- https://issues.apache.org/jira/browse/MAPREDUCE-4052
            if (Shell.WINDOWS) {
                {
                    Field field = Shell.class.getDeclaredField("WINDOWS");
                    field.setAccessible(true);
                    Field modifiersField = Field.class.getDeclaredField("modifiers");
                    modifiersField.setAccessible(true);
                    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
                    field.set(null, false);
                }
                {
                    Field field = java.io.File.class.getDeclaredField("pathSeparator");
                    field.setAccessible(true);
                    Field modifiersField = Field.class.getDeclaredField("modifiers");
                    modifiersField.setAccessible(true);
                    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
                    field.set(null, ":");
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void overrideDevJobJarLocations() {
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        File devJobJar = findFile("../assembly/target", "kylin-assembly-.*-SNAPSHOT-job.jar");
        if (devJobJar != null) {
            conf.overrideMRJobJarPath(devJobJar.getAbsolutePath());
        }
        File parquetJobJar = findFile("../parquet-assembly/target", "parquet-assembly-.*-SNAPSHOT-job.jar");
        if (parquetJobJar != null) {
            conf.overrideKylinParquetJobJarPath(parquetJobJar.getAbsolutePath());
        }
        File devCoprocessorJar = findFile("../storage-hbase/target", "kylin-storage-hbase-.*-SNAPSHOT-coprocessor.jar");
        if (devCoprocessorJar != null) {
            conf.overrideCoprocessorLocalJar(devCoprocessorJar.getAbsolutePath());
        }
    }

    private static File findFile(String dir, String ptn) {
        File[] files = new File(dir).listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.getName().matches(ptn))
                    return f;
            }
        }
        return null;
    }

    public static void authKrb5() {
        // The system property "java.security.krb5.conf" should be set
        try {
            UserGroupInformation.loginUserFromKeytab(
                    System.getProperty("java.security.krb5.principal"),
                    System.getProperty("java.security.krb5.keytab"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        setupDebugEnv();

        int port = 7070;
        if (args.length >= 1) {
            port = Integer.parseInt(args[0]);
        }

        File webBase = new File("../webapp/app");
        File webInfDir = new File(webBase, "WEB-INF");
        FileUtils.deleteDirectory(webInfDir);
        FileUtils.copyDirectoryToDirectory(new File("../server/src/main/webapp/WEB-INF"), webBase);

        Tomcat tomcat = new Tomcat();
        tomcat.setPort(port);
        tomcat.setBaseDir(".");

        // Add AprLifecycleListener
        StandardServer server = (StandardServer) tomcat.getServer();
        AprLifecycleListener listener = new AprLifecycleListener();
        server.addLifecycleListener(listener);

        Context webContext = tomcat.addWebapp("/kylin", webBase.getAbsolutePath());
        ErrorPage notFound = new ErrorPage();
        notFound.setErrorCode(404);
        notFound.setLocation("/index.html");
        webContext.addErrorPage(notFound);
        webContext.addWelcomeFile("index.html");

        // tomcat start
        tomcat.start();
        tomcat.getServer().await();
    }

}
