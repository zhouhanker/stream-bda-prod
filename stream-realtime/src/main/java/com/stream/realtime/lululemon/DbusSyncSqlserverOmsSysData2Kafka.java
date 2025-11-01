package com.stream.realtime.lululemon;

import com.google.gson.JsonObject;
import com.stream.core.ConfigUtils;
import com.stream.core.EnvironmentSettingUtils;
import com.stream.core.KafkaUtils;
import com.stream.realtime.lululemon.func.MapMergeJsonData;
import com.stream.realtime.lululemon.func.ProcessFixJsonData;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.Properties;


/**
 * @Package com.stream.realtime.lululemon.DbusSyncSqlserverOmsSysData2Kafka
 * @Author zhou.han
 * @Date 2025/10/24 18:08
 * @description: Flink Task SQLServer Data To Kafka Topic & TASK 01
 */
public class DbusSyncSqlserverOmsSysData2Kafka {

    private static final String KAFKA_BOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC = "realtime_v3_order_info";
    private static final OutputTag<String> ERROR_PARSE_JSON_DATA_TAG =  new OutputTag<String>("ERROR_PARSE_JSON_DATA_TAG"){};
    private static final String FLINK_UID_VERSION = "_v1";

    @SneakyThrows
    public static void main(String[] args) {

        boolean kafkaTopicDelFlag = KafkaUtils.kafkaTopicExists(KAFKA_BOTSTRAP_SERVERS, OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC);
        KafkaUtils.createKafkaTopic(KAFKA_BOTSTRAP_SERVERS,OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC,3,(short)1,kafkaTopicDelFlag);

        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);


        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode", "initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.put("snapshot.locking.mode", "none");
        debeziumProperties.put("snapshot.fetch.size", 200);
        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("10.160.60.14")
                .port(1433)
                .username("sa")
                .password("zh1028,./")
                .database("realtime_v3")
                .tableList("dbo.oms_order_dtl")
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        DataStreamSource<String> dataStreamSource = env.addSource(sqlServerSource, "_transaction_log_source1");

        SingleOutputStreamOperator<JsonObject> fixJsonDs = dataStreamSource.process(new ProcessFixJsonData(ERROR_PARSE_JSON_DATA_TAG))
                .uid("_processFixJsonAndConvertStr2JsonDs"+FLINK_UID_VERSION)
                .name("processFixJsonAndConvertStr2JsonDs");


        SingleOutputStreamOperator<JsonObject> resultJsonDs = fixJsonDs.map(new MapMergeJsonData())
                .uid("_MapMergeJsonData"+FLINK_UID_VERSION)
                .name("MapMergeJsonData");

        fixJsonDs.getSideOutput(ERROR_PARSE_JSON_DATA_TAG).print("ERROR_PARSE_JSON_DATA_TAG: ");

        SingleOutputStreamOperator<String> jsonobj2strDs = resultJsonDs.map(JsonObject::toString)
                .uid("_jsonobj2str" + FLINK_UID_VERSION)
                .name("jsonobj2str");


        jsonobj2strDs.print("Sink To Kafka Data: -> ");
        jsonobj2strDs.sinkTo(
                KafkaUtils.buildKafkaSinkOrigin(KAFKA_BOTSTRAP_SERVERS, OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC)
        );


        env.execute("DbusSyncSqlserverOmsSysData2Kafka");
    }


}
