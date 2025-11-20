package com.stream.realtime.lululemon;

import com.google.gson.JsonObject;
import com.stream.core.ConfigUtils;
import com.stream.core.EnvironmentSettingUtils;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import com.stream.realtime.lululemon.func.*;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.stream.realtime.lululemon.DbusUserPortraitLabelV2
 * @Author zhou.han
 * @Date 2025/11/17 17:34
 * @description: User Label
 */
public class DbusUserPortraitLabelV2 {

    private static final String KAFKA_LOG_TOPIC = "realtime_v3_logs";
    private static final String KAFKA_BOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String MSSQL_HOST = ConfigUtils.getString("mssql.host");
    private static final int MSSQL_PORT = ConfigUtils.getInt("mssql.port");
    private static final String MSSQL_USER = ConfigUtils.getString("mssql.username");
    private static final String MSSQL_PWD = ConfigUtils.getString("mssql.pwd");
    private static final String MSSQL_DB = ConfigUtils.getString("mssql.realtime_v3.database");
    private static final String MSSQL_TBL = ConfigUtils.getString("mssql.realtime_v3.comment.tbl");

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");
        Configuration conf = new Configuration();
        conf.setString("taskmanager.memory.managed.size", "8g");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        EnvironmentSettingUtils.defaultParameter(env);

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode", "initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.put("snapshot.locking.mode", "none");
        debeziumProperties.put("snapshot.fetch.size", 200);

        DebeziumSourceFunction<String> sqlServerCommentSourceDs = SqlServerSource.<String>builder()
                .hostname(MSSQL_HOST)
                .port(MSSQL_PORT)
                .username(MSSQL_USER)
                .password(MSSQL_PWD)
                .database(MSSQL_DB)
                .tableList(MSSQL_TBL)
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        SingleOutputStreamOperator<String> commentDs = env.addSource(sqlServerCommentSourceDs)
                .uid("_readSqlserverCommentDs")
                .name("_readSqlserverCommentDs");

        DataStreamSource<String> originKafkaLogDs = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(KAFKA_BOTSTRAP_SERVERS, KAFKA_LOG_TOPIC, new Date().toString(), OffsetsInitializer.earliest()),
                WaterMarkUtils.publicAssignWatermarkStrategyUseGsonParse("ts", 5L),
                "_log_kafka_source_realtime_v3_logs"
        );

        // {"log_id":"9abfcfa78c4f48a9adf7d20e833cbf94","device":{"brand":"honor","plat":"android","platv":"13","softv":"7.84.0","uname":"","userkey":"4eabe9a6f25a4070","device":"any-an00"},"gis":{"ip":"60.233.17.89"},"network":{"net":"wifi"},"opa":"adinfo","log_type":"search","ts":1763213217186.531,"product_id":"10158212569217","order_id":"4fbcc8c583c74fa79bb85cba15224cd1","user_id":"17ece09e-d3c0-4fb4-a510-d9406771e987","keywords":["背心"],"ds":"20251115","key_column":"17ece09e-d3c0-4fb4-a510-d9406771e987_20251115"}
        SingleOutputStreamOperator<JsonObject> resultLogDs = originKafkaLogDs.map(new MapConvertLogOriginAndGetDsTimeFunc())
                .uid("_convertKafkaOrigin2JsonDs")
                .name("convertKafkaOrigin2JsonDs");

        SingleOutputStreamOperator<JsonObject> comment2JsonDs = commentDs.map(new MapConvertSqlServerCommentFunc())
                .uid("_comment2Json")
                .name("_comment2Json");

        // {"user_id":"ba0f9f0e-4fd9-470d-a034-e33cc1390ec9","ds":"20251119","login_time":["2025-11-19 10:23:35","2025-11-19 10:23:34","2025-11-19 10:23:33"],"device_info":[{"brand":"iphone","plat":"iphone","platv":"17.6.1","softv":"7.36.0","device":"iphone14_3"},{"brand":"huawei","plat":"android","platv":"10","softv":"7.84.0","device":"glk-al00"}],"search_info":["运动头带","瑜伽裤","斜挎包","运动套装","休闲衫","羽绒","瑜伽服","冬装","发圈"],"gis":[{"ip":"112.49.191.247"},{"ip":"117.154.192.144"}]}
        SingleOutputStreamOperator<JsonObject> keyedUserLogsAggDs = resultLogDs.filter(data -> data.has("user_id") && data.has("ds"))
                .keyBy(data -> data.get("key_column").getAsString())
                .process(new KeyedProcessUserAggMergeFunc())
                .uid("_KeyedProcessUserAggMergeFunc")
                .name("KeyedProcessUserAggMergeFunc");


        SingleOutputStreamOperator<JsonObject> asyncHbaseUserInfoDs = AsyncDataStream.unorderedWait(
                        keyedUserLogsAggDs,
                        new AsyncHbaseDimUserInfoFunc(),
                        60,
                        TimeUnit.MINUTES,
                        500
                ).uid("_supHbaseDimUserInfoAsync")
                .name("supHbaseDimUserInfoAsync");

        SingleOutputStreamOperator<JsonObject> checkUserCommentSenDs = comment2JsonDs.map(new MapSensitiveWordFunc())
                .uid("_checkUserCommentSensitiveWord")
                .name("checkUserCommentSensitiveWord");

        checkUserCommentSenDs.keyBy(data -> data.get("user_id").getAsString() + "_" + data.get("ds").getAsString())
                        .process(new KeyedProcessSensitiveWordAggFunc())
                                .print();


        env.execute();
    }
}
