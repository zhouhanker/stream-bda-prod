package com.stream.realtime.lululemon;

import com.google.gson.JsonObject;
import com.stream.core.ConfigUtils;
import com.stream.core.EnvironmentSettingUtils;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import com.stream.realtime.lululemon.func.KeyedProcessUserAggMergeFunc;
import com.stream.realtime.lululemon.func.MapConvertLogOriginAndGetDsTimeFunc;
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

        // {"log_id":"9abfcfa78c4f48a9adf7d20e833cbf94","device":{"brand":"honor","plat":"android","platv":"13","softv":"7.84.0","uname":"","userkey":"4eabe9a6f25a4070","device":"any-an00"},"gis":{"ip":"60.233.17.89"},"network":{"net":"wifi"},"opa":"adinfo","log_type":"search","ts":1763213217186.531,"product_id":"10158212569217","order_id":"4fbcc8c583c74fa79bb85cba15224cd1","user_id":"17ece09e-d3c0-4fb4-a510-d9406771e987","keywords":["背心"],"ds":"20251115","key_column":"17ece09e-d3c0-4fb4-a510-d9406771e987_20251115"}
        SingleOutputStreamOperator<JsonObject> resultLogDs = originKafkaLogDs.map(new MapConvertLogOriginAndGetDsTimeFunc())
                .uid("_convertKafkaOrigin2JsonDs")
                .name("convertKafkaOrigin2JsonDs");

        SingleOutputStreamOperator<JsonObject> keyedUserAggDs = resultLogDs.filter(data -> data.has("user_id") && data.has("ds"))
                .keyBy(data -> data.get("key_column").getAsString())
                .process(new KeyedProcessUserAggMergeFunc())
                .uid("_KeyedProcessUserAggMergeFunc")
                .name("KeyedProcessUserAggMergeFunc");




        env.execute();
    }
}
