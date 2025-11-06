package com.stream.realtime.lululemon.utils;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;

import java.util.Date;
import java.util.Properties;

/**
 * @Package com.stream.realtime.lululemon.utils
 * @Author zhou.han
 * @Date 2025/11/05
 * @Desc 优化后的 DorisSinkUtils：提取公共配置，支持主键模型和明细模型
 */
public class DorisSinkUtils {

    /**
     * 构建主键模型 Sink（Unique Key + Merge-on-Write）
     */
    public static DorisSink<String> buildDorisPrimaryModelKeyUpdateSink(
            String feNodes, String tableName, String username, String password,
            int bufferCount, int bufferSize) {

        Properties props = baseProperties();
        // 主键模型可能需要 MERGE 或 DELETE 支持，这里可按需打开
        // props.setProperty("merge_type", "MERGE");

        return buildBaseDorisSink(
                feNodes, tableName, username, password,
                bufferCount, bufferSize, props,
                "pk_"
        );
    }

    /**
     * 构建明细模型 Sink（Duplicate Key）
     */
    public static DorisSink<String> buildDorisDuplicateModelSink(
            String feNodes, String tableName, String username, String password,
            int bufferCount, int bufferSize) {

        Properties props = baseProperties();
        // 明细模型仅插入，不需要 merge/delete 配置
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
