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

package flink.examples.test;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.shaded.guava18.com.google.common.base.Preconditions;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

/** Common Options for HBase. */
@Internal
public class HBaseOptions {

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of HBase table to connect.");

    public static final ConfigOption<String> BDP_HBASE_INSTANCE_NAME =
            ConfigOptions.key("bdp.hbase.instance.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Bdp hbase instance name.");

    public static final ConfigOption<String> BDP_HBASE_ACCESSKEY =
            ConfigOptions.key("bdp.hbase.accesskey")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Bdp hbase access key.");

    public static final ConfigOption<String> BDP_HBASE_ERP =
            ConfigOptions.key("bdp.hbase.erp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Bdp hbase erp.");

    public static final ConfigOption<String> ZOOKEEPER_QUORUM =
            ConfigOptions.key("zookeeper.quorum")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The HBase Zookeeper quorum.");

    public static final ConfigOption<String> ZOOKEEPER_ZNODE_PARENT =
            ConfigOptions.key("zookeeper.znode.parent")
                    .stringType()
                    .defaultValue("/hbase")
                    .withDescription("The root dir in Zookeeper for HBase cluster.");

    public static final ConfigOption<String> NULL_STRING_LITERAL =
            ConfigOptions.key("null-string-literal")
                    .stringType()
                    .defaultValue("null")
                    .withDescription(
                            "Representation for null values for string fields. HBase source and "
                                    + "sink encodes/decodes empty bytes as null values for all types except string type.");

    public static final ConfigOption<MemorySize> SINK_BUFFER_FLUSH_MAX_SIZE =
            ConfigOptions.key("sink.buffer-flush.max-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("2mb"))
                    .withDescription(
                            "Writing option, maximum size in memory of buffered rows for each "
                                    + "writing request. This can improve performance for writing data to HBase database, "
                                    + "but may increase the latency. Can be set to '0' to disable it. ");

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Writing option, maximum number of rows to buffer for each writing request. "
                                    + "This can improve performance for writing data to HBase database, but may increase the latency. "
                                    + "Can be set to '0' to disable it.");

    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "Writing option, the interval to flush any buffered rows. "
                                    + "This can improve performance for writing data to HBase database, but may increase the latency. "
                                    + "Can be set to '0' to disable it. Note, both 'sink.buffer-flush.max-size' and 'sink.buffer-flush.max-rows' "
                                    + "can be set to '0' with the flush interval set allowing for complete async processing of buffered actions.");

    public static final ConfigOption<Boolean> SINK_WRITE_DELETE_DATA =
            ConfigOptions.key("sink.write.delete.data")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Writing option.Write delete data or not.");

    public static final ConfigOption<MemorySize> CONNECTOR_WRITE_BUFFER_FLUSH_MAX_SIZE =
            ConfigOptions.key("hbase.write.buffer-flush.max-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("2mb"))
                    .withDescription(
                            "Writing option, maximum size in memory of buffered rows for each "
                                    + "writing request. This can improve performance for writing data to HBase database, "
                                    + "but may increase the latency. Can be set to '0' to disable it. ");

    public static final ConfigOption<Integer> CONNECTOR_WRITE_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("hbase.write.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Writing option, maximum number of rows to buffer for each writing request. "
                                    + "This can improve performance for writing data to HBase database, but may increase the latency. "
                                    + "Can be set to '0' to disable it.");

    public static final ConfigOption<Duration> CONNECTOR_WRITE_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("hbase.write.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "Writing option, the interval to flush any buffered rows. "
                                    + "This can improve performance for writing data to HBase database, but may increase the latency. "
                                    + "Can be set to '0' to disable it. Note, both 'sink.buffer-flush.max-size' and 'sink.buffer-flush.max-rows' "
                                    + "can be set to '0' with the flush interval set allowing for complete async processing of buffered actions.");

    public static final String HEAP = "heap";
    public static final String OFF_HEAP = "off-heap";

    public static final ConfigOption<String> CACHE_TYPE =
            ConfigOptions.key("cache.type")
                    .stringType()
                    .defaultValue(HEAP)
                    .withDescription("Cache type.");

    public static final ConfigOption<Integer> CACHE_MAX_ROWS =
            ConfigOptions.key("cache.max-rows")
                    .intType()
                    .defaultValue(0)
                    .withDescription("Cache max rows.");

    public static final ConfigOption<Duration> CACHE_TTL =
            ConfigOptions.key("cache.ttl")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(0))
                    .withDescription("Cache ttl.");

    public static final ConfigOption<Integer> CACHE_MAX_RETRIES =
            ConfigOptions.key("cache.max-retries")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Cache max retries.");

    public static final ConfigOption<Boolean> CACHE_PENETRATION_PREVENT =
            ConfigOptions.key("cache.penetration.prevent")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Prevent cache penetration.");

    public static final ConfigOption<Boolean> LOOKUP_ASYNC =
            ConfigOptions.key("lookup.async")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("HBase lookup async.");

    // Prefix for HBase specific properties.
    public static final String PROPERTIES_PREFIX = "properties.";

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------



    /**
     * config HBase Configuration.
     *
     * @param options properties option
     */
    public static Configuration getHBaseConfiguration(Map<String, String> options) {
        org.apache.flink.configuration.Configuration tableOptions =
                org.apache.flink.configuration.Configuration.fromMap(options);
        // create default configuration from current runtime env (`hbase-site.xml` in classpath)
        // first,
        Configuration hbaseClientConf = HBaseConfigurationUtil.getHBaseConfiguration();
        hbaseClientConf.set(HConstants.ZOOKEEPER_QUORUM, tableOptions.getString(ZOOKEEPER_QUORUM));
        hbaseClientConf.set(
                HConstants.ZOOKEEPER_ZNODE_PARENT, tableOptions.getString(ZOOKEEPER_ZNODE_PARENT));
        // add HBase properties
        final Properties properties = getHBaseClientProperties(options);
        properties.forEach((k, v) -> hbaseClientConf.set(k.toString(), v.toString()));
        return hbaseClientConf;
    }

    public static Configuration getHBaseConf(Map<String, String> options) {
        org.apache.flink.configuration.Configuration tableOptions =
                org.apache.flink.configuration.Configuration.fromMap(options);
        Configuration conf = new Configuration();
        conf.set(BDP_HBASE_INSTANCE_NAME.key(), tableOptions.getString(BDP_HBASE_INSTANCE_NAME));
        conf.set(BDP_HBASE_ACCESSKEY.key(), tableOptions.getString(BDP_HBASE_ACCESSKEY));
        if (!Strings.isNullOrEmpty(tableOptions.getString(BDP_HBASE_ERP))) {
            conf.set(BDP_HBASE_ERP.key(), tableOptions.getString(BDP_HBASE_ERP));
        }
        // add HBase properties
        final Properties properties = getHBaseClientProperties(options);
        properties.forEach((k, v) -> conf.set(k.toString(), v.toString()));
        return conf;
    }

    // get HBase table properties
    private static Properties getHBaseClientProperties(Map<String, String> tableOptions) {
        final Properties hbaseProperties = new Properties();

        if (containsHBaseClientProperties(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                    .forEach(
                            key -> {
                                final String value = tableOptions.get(key);
                                final String subKey = key.substring((PROPERTIES_PREFIX).length());
                                hbaseProperties.put(subKey, value);
                            });
        }
        return hbaseProperties;
    }

    /** Returns wether the table options contains HBase client properties or not. 'properties'. */
    private static boolean containsHBaseClientProperties(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
    }

    public static void validateCacheProperties(Map<String, String> options) {
        org.apache.flink.configuration.Configuration tableOptions =
                org.apache.flink.configuration.Configuration.fromMap(options);
        String cacheType = tableOptions.getString(CACHE_TYPE);
        Preconditions.checkArgument(
                cacheType.equals(HEAP) || cacheType.equals(OFF_HEAP),
                "Dim cache only heap or off-heap. Current set is " + cacheType + ".");
    }


}
