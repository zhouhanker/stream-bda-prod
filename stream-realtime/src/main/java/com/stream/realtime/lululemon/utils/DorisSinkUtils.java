package com.stream.realtime.lululemon.utils;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;

import java.util.Date;
import java.util.Properties;

/**
 *  DorisSinkUtils：支持写入前清空表数据
 */
public class DorisSinkUtils {

    /**
     * 构建主键模型 Sink（Unique Key + Merge-on-Write）
     * @param clearBeforeLoad 是否写入前清空表
     */
    public static DorisSink<String> buildDorisPrimaryModelKeyUpdateSink(
            String feNodes, String tableName, String username, String password,
            int bufferCount, int bufferSize, boolean clearBeforeLoad) {

        Properties props = baseProperties();
        applyClearBeforeLoad(props, clearBeforeLoad);

        return buildBaseDorisSink(
                feNodes, tableName, username, password,
                bufferCount, bufferSize, props,
                "pk_"
        );
    }

    /**
     * 构建明细模型 Sink（Duplicate Key）
     * @param clearBeforeLoad 是否写入前清空表
     */
    public static DorisSink<String> buildDorisDuplicateModelSink(
            String feNodes, String tableName, String username, String password,
            int bufferCount, int bufferSize, boolean clearBeforeLoad) {

        Properties props = baseProperties();
        applyClearBeforeLoad(props, clearBeforeLoad);

        return buildBaseDorisSink(
                feNodes, tableName, username, password,
                bufferCount, bufferSize, props,
                "dtl_"
        );
    }

    /**
     * 公共 Doris Sink 构建逻辑
     */
    private static DorisSink<String> buildBaseDorisSink(
            String feNodes, String tableName, String username, String password,
            int bufferCount, int bufferSize, Properties props, String labelPrefix) {

        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes(feNodes)
                .setTableIdentifier(tableName)
                .setUsername(username)
                .setPassword(password)
                .build();

        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                .setLabelPrefix(labelPrefix + new Date().getTime())
                .disable2PC()
                .setBufferCount(bufferCount)
                .setBufferSize(bufferSize)
                .setMaxRetries(3)
                .setStreamLoadProp(props)
                .setDeletable(false)
                .build();

        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(dorisOptions)
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new SimpleStringSerializer())
                .build();
    }

    /**
     * 设置是否在写入前清空表
     */
    private static void applyClearBeforeLoad(Properties props, boolean clearBeforeLoad) {
        if (clearBeforeLoad) {
            props.setProperty("exec_mem_limit", "2147483648");
            props.setProperty("truncate_table", "true");
        }
    }

    /**
     * 基础通用 Stream Load 属性
     */
    private static Properties baseProperties() {
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        props.setProperty("strict_mode", "true");
        props.setProperty("max_filter_ratio", "0.1");
        props.setProperty("strip_outer_array", "false");
        props.setProperty("timeout", "60000");
        return props;
    }

    private DorisSinkUtils() {}
}
