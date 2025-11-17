package com.stream.realtime.lululemon;

import com.google.gson.JsonObject;
import com.stream.core.ConfigUtils;
import com.stream.core.EnvironmentSettingUtils;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import com.stream.realtime.lululemon.func.MapConvertLogOriginAndGetDsTimeFunc;
import com.stream.realtime.lululemon.func.keyedProcessMargeFunc;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;

/**
 * @Package com.stream.realtime.lululemon.DbusUserPortraitLabelV2
 * @Author zhou.han
 * @Date 2025/11/17 17:34
 * @description: 用户画像 标签
 */
public class DbusUserPortraitLabelV2 {

    private static final String KAFKA_LOG_TOPIC = "realtime_v3_logs";
    private static final String KAFKA_BOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","root");
        Configuration conf = new Configuration();
        conf.setString("taskmanager.memory.managed.size", "8g");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String> originKafkaLogDs = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(KAFKA_BOTSTRAP_SERVERS, KAFKA_LOG_TOPIC, new Date().toString(), OffsetsInitializer.earliest()),
                WaterMarkUtils.publicAssignWatermarkStrategyUseGsonParse("ts", 5L),
                "_log_kafka_source_realtime_v3_logs"
        );

        // lambda 表达式  或者叫做链式调用
        SingleOutputStreamOperator<JsonObject> resultLogDs = originKafkaLogDs.map(new MapConvertLogOriginAndGetDsTimeFunc())
                .uid("_convertKafkaOrigin2JsonDs")
                .name("convertKafkaOrigin2JsonDs");

        resultLogDs.filter(data -> data.has("user_id") && data.has("ds"))
                        .keyBy(data -> data.get("key_column").getAsString())
                                .process(new keyedProcessMargeFunc()).print();


        env.execute();
    }
}
