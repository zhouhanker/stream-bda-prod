package com.stream.realtime.lululemon;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.stream.core.*;
import com.stream.realtime.lululemon.func.*;
import com.stream.realtime.lululemon.utils.DorisSinkUtils;
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
    private static final String DORIS_FE_IP = ConfigUtils.getString("doris.fe.ip");
    private static final String DORIS_LOG_TABLE_NAME = ConfigUtils.getString("doris.log.device.table");
    private static final String DORIS_REGION_TABLE_NAME = ConfigUtils.getString("doris.log.region.table");
    private static final String DORIS_USERNAME = ConfigUtils.getString("doris.user.name");
    private static final String DORIS_PASSWORD = ConfigUtils.getString("doris.user.password");
    private static final int DORIS_BUFFER_COUNT = 2;
    private static final int DORIS_BUFFER_SIZE = 1024;

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

        // parse ip
        SingleOutputStreamOperator<JsonObject> convertIp2JsonDs = convert2Json.map(new MapGisMessage2RegionFunc())
                .uid("_mapIp2Region")
                .name("mapIp2Region");

        // 每个页面的访问量 & 聚合到userid
        SingleOutputStreamOperator<String> singleViewAccDs = convertIp2JsonDs
                .keyBy(data -> DateTimeUtils.tsToDate(data.get("ts").getAsLong()) + "|" + data.get("log_type").getAsString())
                .process(new KeyedProcessSingleViewAccFunc())
                .uid("_singleViewAcc")
                .name("singleViewAcc");

        // 计算天和历史天的搜索词统计
        SingleOutputStreamOperator<String> searchTopNAccDs = convertIp2JsonDs.filter(data -> data.has("keywords"))
                .flatMap(new FlatMapKeyWordsArrFunc())
                .returns(Types.GENERIC(JsonObject.class))
                .keyBy(data -> DateTimeUtils.tsToDate(data.get("ts").getAsLong()) + "|" + data.get("keyword").getAsString())
                .process(new KeyedProcessSearchTOPNFunc())
                .uid("_day_searchTOPN")
                .name("day_searchTOPN");

        // 计算region 地区热点
        SingleOutputStreamOperator<JsonObject> computeRegionDs = convertIp2JsonDs.keyBy(data -> DateTimeUtils.tsToDate(data.get("ts").getAsLong()) + "|" + data.get("region").getAsString())
                .process(new KeyedProcessRegionHeatFunc())
                .uid("_compute_region")
                .name("compute_region");


        SingleOutputStreamOperator<JsonObject> deviceStatsDs = convertIp2JsonDs.keyBy(data -> {
                    JsonObject dev = data.getAsJsonObject("device");
                    String os = dev.has("plat") ? dev.get("plat").getAsString().toLowerCase() : "unknown";
                    String brand = dev.has("brand") ? dev.get("brand").getAsString().toLowerCase() : "unknown";
                    String platv = dev.has("platv") ? dev.get("platv").getAsString().toLowerCase() : "unk";
                    return DateTimeUtils.tsToDate(data.get("ts").getAsLong()) + "|" + os + "|" + brand + "|" + platv;
                }).process(new KeyedProcessDeviceStatsFunc())
                .uid("_ProcessDeviceStatsFunc")
                .name("ProcessDeviceStatsFunc");

        deviceStatsDs.map(new MapDevice2DorisColumnFunc())
        .sinkTo(
                DorisSinkUtils.buildPrimaryKeyUpdateSink(DORIS_FE_IP,DORIS_LOG_TABLE_NAME,DORIS_USERNAME,DORIS_PASSWORD,DORIS_BUFFER_COUNT,DORIS_BUFFER_SIZE)
        );

        computeRegionDs.map(new MapRegionData2DorisColumnFunc())
        .sinkTo(
                DorisSinkUtils.buildPrimaryKeyUpdateSink(DORIS_FE_IP,DORIS_REGION_TABLE_NAME,DORIS_USERNAME,DORIS_PASSWORD,DORIS_BUFFER_COUNT,DORIS_BUFFER_SIZE)
        );




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
