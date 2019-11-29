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

package org.apache.kylin.common;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.threadlocal.InternalThreadLocal;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.OrderedProperties;
import org.apache.kylin.common.util.VersionUtil;
import org.apache.zookeeper.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class KylinConfig extends KylinConfigBase {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(KylinConfig.class);
    private static final String METADATA_URI_PREFIX = "Metadata uri : ";

    /**
     * Kylin properties file name
     */
    public static final String KYLIN_CONF_PROPERTIES_FILE = "kylin.properties";
    public static final String KYLIN_DEFAULT_CONF_PROPERTIES_FILE = "kylin-defaults.properties";
    public static final String KYLIN_CONF = "KYLIN_CONF";

    // static cached instances
    private static KylinConfig SYS_ENV_INSTANCE = null;

    // static default Ordered Properties, only need load from classpath once
    private static OrderedProperties defaultOrderedProperties = new OrderedProperties();

    // thread-local instances, will override SYS_ENV_INSTANCE
    private static transient InternalThreadLocal<KylinConfig> THREAD_ENV_INSTANCE = new InternalThreadLocal<>();

    static {
        /*
         * Make Calcite to work with Unicode.
         *
         * Sets default char set for string literals in SQL and row types of
         * RelNode. This is more a label used to compare row type equality. For
         * both SQL string and row record, they are passed to Calcite in String
         * object and does not require additional codec.
         *
         * Ref SaffronProperties.defaultCharset
         * Ref SqlUtil.translateCharacterSetName()
         * Ref NlsString constructor()
         */
        // copied from org.apache.calcite.util.ConversionUtil.NATIVE_UTF16_CHARSET_NAME
        String NATIVE_UTF16_CHARSET_NAME = (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) ? "UTF-16BE" : "UTF-16LE";
        System.setProperty("saffron.default.charset", NATIVE_UTF16_CHARSET_NAME);
        System.setProperty("saffron.default.nationalcharset", NATIVE_UTF16_CHARSET_NAME);
        System.setProperty("saffron.default.collation.name", NATIVE_UTF16_CHARSET_NAME + "$en_US");

    }

    public static File getKylinHomeAtBestEffort() {
        String kylinHome = KylinConfig.getKylinHome();
        if (kylinHome != null) {
            return new File(kylinHome).getAbsoluteFile();
        } else {
            File confFile = KylinConfig.getSitePropertiesFile();
            return confFile.getAbsoluteFile().getParentFile().getParentFile();
        }
    }

    /**
     * Build default ordered properties from classpath, due to those files exist in core-common.jar, no need to load them each time.
     */
    private static void buildDefaultOrderedProperties() {
        // 1. load default configurations from classpath.
        // we have a kylin-defaults.properties in kylin/core-common/src/main/resources
        try {
            URL resource = Thread.currentThread().getContextClassLoader()
                    .getResource(KYLIN_DEFAULT_CONF_PROPERTIES_FILE);
            Preconditions.checkNotNull(resource);
            logger.info("Loading kylin-defaults.properties from {}", resource.getPath());
            loadPropertiesFromInputStream(resource.openStream(), defaultOrderedProperties);

            // 2. load additional default configurations from classpath.
            // This is old logic, will load kylin-defaults(0~9).properties in kylin/core-common/src/main/resources
            // Suggest remove this logic if no needed.
            for (int i = 0; i < 10; i++) {
                String fileName = "kylin-defaults" + (i) + ".properties";
                URL additionalResource = Thread.currentThread().getContextClassLoader().getResource(fileName);
                if (additionalResource != null) {
                    logger.info("Loading {} from {} ", fileName, additionalResource.getPath());
                    loadPropertiesFromInputStream(additionalResource.openStream(), defaultOrderedProperties);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static KylinConfig getInstanceFromEnv() {
        synchronized (KylinConfig.class) {
            KylinConfig config = THREAD_ENV_INSTANCE.get();
            if (config != null) {
                return config;
            }

            if (SYS_ENV_INSTANCE == null) {
                try {
                    //build default ordered properties will only be called once.
                    //This logic no need called by CoProcessor due to it didn't call getInstanceFromEnv.
                    buildDefaultOrderedProperties();

                    config = new KylinConfig();
                    config.reloadKylinConfig(buildSiteProperties());
                    VersionUtil.loadKylinVersion();
                    logger.info("Initialized a new KylinConfig from getInstanceFromEnv : "
                            + System.identityHashCode(config));
                    SYS_ENV_INSTANCE = config;
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("Failed to find KylinConfig ", e);
                }
            }
            return SYS_ENV_INSTANCE;
        }
    }

    // Only used in test cases!!!
    public static void destroyInstance() {
        synchronized (KylinConfig.class) {
            logger.info("Destroy KylinConfig");
            dumpStackTrace();
            SYS_ENV_INSTANCE = null;
            THREAD_ENV_INSTANCE = new InternalThreadLocal<>();
        }
    }

    private static void dumpStackTrace() {

        //uncomment below to start debugging

        //        Thread t = Thread.currentThread();
        //        int maxStackTraceDepth = 20;
        //        int current = 0;
        //
        //        StackTraceElement[] stackTrace = t.getStackTrace();
        //        StringBuilder buf = new StringBuilder("This is not a exception, just for diagnose purpose:");
        //        buf.append("\n");
        //        for (StackTraceElement e : stackTrace) {
        //            if (++current > maxStackTraceDepth) {
        //                break;
        //            }
        //            buf.append("\t").append("at ").append(e.toString()).append("\n");
        //        }
        //        logger.info(buf.toString());
    }

    public enum UriType {
        PROPERTIES_FILE, REST_ADDR, LOCAL_FOLDER, HDFS_FILE
    }

    private static UriType decideUriType(String metaUri) {

        try {
            File file = new File(metaUri);
            // for the developers using windows, without this condition, it will never find the file
            if (file.exists() || metaUri.contains("/") || Shell.WINDOWS) {
                if (!file.exists()) {
                    file.mkdirs();
                }
                if (file.isDirectory()) {
                    return UriType.LOCAL_FOLDER;
                } else if (file.isFile()) {
                    if (file.getName().equalsIgnoreCase(KYLIN_CONF_PROPERTIES_FILE)) {
                        return UriType.PROPERTIES_FILE;
                    } else {
                        throw new IllegalStateException(
                                METADATA_URI_PREFIX + metaUri + " is a local file but not kylin.properties");
                    }
                } else {
                    throw new IllegalStateException(METADATA_URI_PREFIX + metaUri
                            + " looks like a file but it's neither a file nor a directory");
                }
            } else {
                if (RestClient.matchFullRestPattern(metaUri))
                    return UriType.REST_ADDR;
                else
                    throw new IllegalStateException(METADATA_URI_PREFIX + metaUri + " is not a valid REST URI address");
            }
        } catch (Exception e) {
            throw new IllegalStateException(METADATA_URI_PREFIX + metaUri + " is not recognized", e);
        }
    }

    public static KylinConfig createInstanceFromUri(String uri) {
        /**
         * --hbase: 1. PROPERTIES_FILE: path to kylin.properties 2. REST_ADDR:
         * rest service resource, format: user:password@host:port --local: 1.
         * LOCAL_FOLDER: path to resource folder
         */
        UriType uriType = decideUriType(uri);

        if (uriType == UriType.LOCAL_FOLDER) {
            KylinConfig config = new KylinConfig();
            config.setMetadataUrl(uri);
            return config;
        } else if (uriType == UriType.PROPERTIES_FILE) {
            KylinConfig config;
            try {
                config = new KylinConfig();
                InputStream is = new FileInputStream(uri);
                Properties prop = streamToProps(is);
                config.reloadKylinConfig(prop);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return config;
        } else {// rest_addr
            try {
                KylinConfig config = new KylinConfig();
                RestClient client = new RestClient(uri);
                String propertyText = client.getKylinProperties();
                InputStream is = IOUtils.toInputStream(propertyText, Charset.defaultCharset());
                Properties prop = streamToProps(is);
                config.reloadKylinConfig(prop);
                return config;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static Properties streamToProps(InputStream is) throws IOException {
        Properties prop = new Properties();
        prop.load(is);
        IOUtils.closeQuietly(is);
        return prop;
    }

    public static void setKylinConfigInEnvIfMissing(Properties prop) {
        synchronized (KylinConfig.class) {
            if (SYS_ENV_INSTANCE == null) {
                try {
                    KylinConfig config = new KylinConfig();
                    config.reloadKylinConfig(prop);
                    logger.info("Resetting SYS_ENV_INSTANCE by a input stream: {}", System.identityHashCode(config));
                    SYS_ENV_INSTANCE = config;
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("Failed to find KylinConfig ", e);
                }
            }
        }
    }

    public static void setKylinConfigInEnvIfMissing(String propsInStr) throws IOException {
        Properties props = new Properties();
        props.load(new StringReader(propsInStr));
        setKylinConfigInEnvIfMissing(props);
    }

    // auto-closeable API to remind that a thread local config must always be removed
    public static SetAndUnsetThreadLocalConfig setAndUnsetThreadLocalConfig(KylinConfig config) {
        return new SetAndUnsetThreadLocalConfig(config);
    }

    public static class SetAndUnsetThreadLocalConfig implements AutoCloseable {

        public SetAndUnsetThreadLocalConfig(KylinConfig config) {
            THREAD_ENV_INSTANCE.set(config);
        }

        @Override
        public void close() {
            THREAD_ENV_INSTANCE.remove();
        }
    }

    public static KylinConfig createKylinConfig(String propsInStr) {
        try {
            Properties props = new Properties();
            props.load(new StringReader(propsInStr));
            return createKylinConfig(props);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create KylinConfig from string: " + propsInStr, e);
        }
    }

    public static KylinConfig createKylinConfig(KylinConfig another) {
        return createKylinConfig(another.getRawAllProperties());
    }

    public static KylinConfig createKylinConfig(Properties prop) {
        KylinConfig kylinConfig = new KylinConfig();
        kylinConfig.reloadKylinConfig(prop);
        return kylinConfig;
    }

    public static File getKylinConfDir() {
        return getSitePropertiesFile().getParentFile();
    }

    // should be private; package visible for test only
    static File getSitePropertiesFile() {
        String kylinConfHome = System.getProperty(KYLIN_CONF);
        if (!StringUtils.isEmpty(kylinConfHome)) {
            logger.info("Use KYLIN_CONF={}", kylinConfHome);
            return existFile(kylinConfHome);
        }

        logger.debug("KYLIN_CONF property was not set, will seek KYLIN_HOME env variable");

        String kylinHome = getKylinHome();
        if (StringUtils.isEmpty(kylinHome))
            throw new KylinConfigCannotInitException("Didn't find KYLIN_CONF or KYLIN_HOME, please set one of them");

        logger.info("Use KYLIN_HOME={}", kylinHome);
        String path = kylinHome + File.separator + "conf";
        return existFile(path);
    }

    /**
     * Return a File only if it exists
     */
    private static File existFile(String path) {
        if (path == null) {
            return null;
        }

        return new File(path, KYLIN_CONF_PROPERTIES_FILE);
    }

    // build kylin properties from site deployment, a.k.a KYLIN_HOME/conf/kylin.properties
    private static Properties buildSiteProperties() {
        Properties conf = new Properties();

        OrderedProperties orderedProperties = buildSiteOrderedProps();
        for (Map.Entry<String, String> each : orderedProperties.entrySet()) {
            conf.put(each.getKey(), each.getValue().trim());
        }

        return conf;
    }

    // build kylin properties from site deployment, a.k.a KYLIN_HOME/conf/kylin.properties
    private static OrderedProperties buildSiteOrderedProps() {

        try {
            // 1. load default configurations from classpath.
            // we have kylin-defaults.properties in kylin/core-common/src/main/resources
            // Load them each time will caused thread block when multiple query request to Kylin
            OrderedProperties orderedProperties = new OrderedProperties();
            orderedProperties.putAll(defaultOrderedProperties);

            // 2. load site conf, to keep backward compatibility it's still named kylin.properties
            // actually it's better to be named kylin-site.properties
            File propFile = getSitePropertiesFile();
            if (propFile == null || !propFile.exists()) {
                logger.error("fail to locate " + KYLIN_CONF_PROPERTIES_FILE + " at '"
                        + (propFile != null ? propFile.getAbsolutePath() : "") + "'");
                throw new RuntimeException("fail to locate " + KYLIN_CONF_PROPERTIES_FILE);
            }
            loadPropertiesFromInputStream(new FileInputStream(propFile), orderedProperties);

            // 3. still support kylin.properties.override as secondary override
            // not suggest to use it anymore
            File propOverrideFile = new File(propFile.getParentFile(), propFile.getName() + ".override");
            if (propOverrideFile.exists()) {
                loadPropertiesFromInputStream(new FileInputStream(propOverrideFile), orderedProperties);
            }
            return orderedProperties;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * will close the passed in inputstream
     */
    private static void loadPropertiesFromInputStream(InputStream inputStream, OrderedProperties properties) {
        Preconditions.checkNotNull(properties);

        try (BufferedReader confReader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            OrderedProperties temp = new OrderedProperties();
            temp.load(confReader);
            temp = BCC.check(temp);

            properties.putAll(temp);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void setSandboxEnvIfPossible() {
        File dir1 = new File("../examples/test_case_data/sandbox");
        File dir2 = new File("../../kylin/examples/test_case_data/sandbox");

        if (dir1.exists()) {
            logger.info("Setting sandbox env, KYLIN_CONF=" + dir1.getAbsolutePath());
            ClassUtil.addClasspath(dir1.getAbsolutePath());
            System.setProperty(KylinConfig.KYLIN_CONF, dir1.getAbsolutePath());
        } else if (dir2.exists()) {
            logger.info("Setting sandbox env, KYLIN_CONF=" + dir2.getAbsolutePath());
            ClassUtil.addClasspath(dir2.getAbsolutePath());
            System.setProperty(KylinConfig.KYLIN_CONF, dir2.getAbsolutePath());
        }
    }

    // ============================================================================

    transient Map<Class, Object> managersCache = new ConcurrentHashMap<>();

    private KylinConfig() {
        super();
    }

    protected KylinConfig(Properties props, boolean force) {
        super(props, force);
    }

    public <T> T getManager(Class<T> clz) {
        KylinConfig base = base();
        if (base != this)
            return base.getManager(clz);

        if (managersCache == null) {
            managersCache = new ConcurrentHashMap<>();
        }

        Object mgr = managersCache.get(clz);
        if (mgr != null)
            return (T) mgr;

        synchronized (clz) {
            mgr = managersCache.get(clz);
            if (mgr != null)
                return (T) mgr;

            try {
                logger.info("Creating new manager instance of {}", clz);

                // new manager via static Manager.newInstance()
                Method method = clz.getDeclaredMethod("newInstance", KylinConfig.class);
                method.setAccessible(true); // override accessibility
                mgr = method.invoke(null, this);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            managersCache.put(clz, mgr);
        }
        return (T) mgr;
    }

    public void clearManagers() {
        KylinConfig base = base();
        if (base != this) {
            base.clearManagers();
            return;
        }

        Map<Class, Closeable> closableManagers = new ConcurrentHashMap<>();

        managersCache.forEach((key, value) -> {
            if (value instanceof Closeable) {
                closableManagers.put(key, (Closeable) value);
            }
        });

        managersCache.clear();

        if (closableManagers.size() > 0) {
            closableManagers.forEach((key, value) -> {
                logger.info("Close manager {}", key.getSimpleName());
                value.close();
            });
        }
    }

    public Properties exportToProperties() {
        Properties all = getAllProperties();
        Properties copy = new Properties();
        copy.putAll(all);
        return copy;
    }

    public String exportAllToString() {
        final Properties allProps = getProperties(null);
        final OrderedProperties orderedProperties = KylinConfig.buildSiteOrderedProps();

        for (Map.Entry<Object, Object> entry : allProps.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            orderedProperties.setProperty(key, value);
        }

        final StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : orderedProperties.entrySet()) {
            sb.append(entry.getKey() + "=" + entry.getValue()).append('\n');
        }
        return sb.toString();

    }

    public String exportToString(Collection<String> propertyKeys) {
        Properties filteredProps = getProperties(propertyKeys);
        OrderedProperties orderedProperties = KylinConfig.buildSiteOrderedProps();

        for (String key : propertyKeys) {
            if (!filteredProps.containsKey(key)) {
                filteredProps.put(key, orderedProperties.getProperty(key, ""));
            }
        }

        final StringBuilder sb = new StringBuilder();
        for (Map.Entry<Object, Object> entry : filteredProps.entrySet()) {
            sb.append(entry.getKey() + "=" + entry.getValue()).append('\n');
        }
        return sb.toString();
    }

    public void exportToFile(File file) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            getAllProperties().store(fos, file.getAbsolutePath());
        }
    }

    public synchronized void reloadFromSiteProperties() {
        reloadKylinConfig(buildSiteProperties());
    }

    public KylinConfig base() {
        return this;
    }

    private int superHashCode() {
        return super.hashCode();
    }

    @Override
    public int hashCode() {
        return base().superHashCode();
    }

    @Override
    public boolean equals(Object another) {
        if (!(another instanceof KylinConfig))
            return false;
        else
            return this.base() == ((KylinConfig) another).base();
    }

    //kylin parquetv2

    public String getCuboidSpanningTree() {
        return this.getOptional("kap.cube.cuboid-spanning-tree",
                "io.kyligence.kap.metadata.cube.cuboid.NForestSpanningTree");
    }

}
