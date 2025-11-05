package com.stream.realtime.lululemon.utils;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;

import java.util.Date;
import java.util.Properties;

/**
 * @Package com.stream.realtime.lululemon.utils.DorisSinkUtils
 * @Author zhou.han
 * @Date 2025/11/5 13:34
 * @description:
 */
public class DorisSinkUtils {

    public static DorisSink<String> buildPrimaryKeyUpdateSink(String feNodes, String tableName, String username, String password, int bufferCount, int bufferSize) {

        // Doris Stream Load 属性配置
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        // 主键更新
//        props.setProperty("merge_type", "MERGE");
        props.setProperty("strict_mode", "true");
        props.setProperty("max_filter_ratio", "0.1");
        props.setProperty("strip_outer_array", "false");
//        props.setProperty("sink.enable-delete", "false");
        props.setProperty("timeout", "60000");

        // 构建 Doris Sink
        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(
                        DorisOptions.builder()
                                .setFenodes(feNodes)
                                .setTableIdentifier(tableName)
                                .setUsername(username)
                                .setPassword(password)
                                .build()
                )
                .setDorisExecutionOptions(
                        DorisExecutionOptions.builder()
                                .setLabelPrefix("flk_" + new Date().getTime())
                                .disable2PC()
                                .setBufferCount(bufferCount)
                                .setBufferSize(bufferSize)
                                .setMaxRetries(3)
                                .setStreamLoadProp(props)
                                .setDeletable(false)
                                .build()
                )
                .setSerializer(new SimpleStringSerializer())
                .build();
    }


    private DorisSinkUtils() {}

}
