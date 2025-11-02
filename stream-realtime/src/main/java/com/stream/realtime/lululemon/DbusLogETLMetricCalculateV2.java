package com.stream.realtime.lululemon;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.stream.core.*;
import com.stream.realtime.lululemon.func.FlatMapKeyWordsArrFunc;
import com.stream.realtime.lululemon.func.KeyedProcessSearchTOPNFunc;
import com.stream.realtime.lululemon.func.KeyedProcessSingleViewAccFunc;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;

/**
 * @Package com.stream.realtime.lululemon.DbusLogETLMetricTask
 * @Author zhou.han
 * @Date 2025/10/31 14:00
 * @description:
 */
public class DbusLogETLMetricCalculateV2 {

    private static final String KAFKA_BOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String KAFKA_LOG_TOPIC = "realtime_v3_logs";

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");
        Configuration conf = new Configuration();
        conf.setString("taskmanager.memory.managed.size", "4g");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String> originKafkaLogDs = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(KAFKA_BOTSTRAP_SERVERS, KAFKA_LOG_TOPIC, new Date().toString(), OffsetsInitializer.earliest()),
                WaterMarkUtils.publicAssignWatermarkStrategyUseGsonParse("ts", 5L),
                "_log_kafka_source_realtime_v3_logs"
        );

        SingleOutputStreamOperator<JsonObject> convert2Json = originKafkaLogDs.map(DbusLogETLMetricCalculateV2::gsonStr2JsonObject)
                .uid("_convertJsonObj")
                .name("convertJsonObj");

        // 每个页面的访问量 & 聚合到userid
        SingleOutputStreamOperator<String> singleViewAccDs = convert2Json
                .keyBy(data -> DateTimeUtils.tsToDate(data.get("ts").getAsLong()) + "|" + data.get("log_type").getAsString())
                .process(new KeyedProcessSingleViewAccFunc())
                .uid("_singleViewAcc")
                .name("singleViewAcc");

        // 计算天和历史天的搜索词统计
        SingleOutputStreamOperator<String> searchTopNAccDs = convert2Json.filter(data -> data.has("keywords"))
                .flatMap(new FlatMapKeyWordsArrFunc())
                .returns(Types.GENERIC(JsonObject.class))
                .keyBy(data -> DateTimeUtils.tsToDate(data.get("ts").getAsLong()) + "|" + data.get("keyword").getAsString())
                .process(new KeyedProcessSearchTOPNFunc())
                .uid("_day_searchTOPN")
                .name("day_searchTOPN");




        env.execute();
    }


    public static JsonObject gsonStr2JsonObject(String s){
        try {
            return JsonParser.parseString(s).getAsJsonObject();
        }catch (Exception e){
            return new JsonObject();
        }
    }


    private static String getKeyword(JsonObject obj) {
        String[] keys = {"keywords", "kw", "q", "word"};
        for (String k : keys) {
            if (obj.has(k)) return obj.get(k).getAsString();
        }
        return "";
    }

}
