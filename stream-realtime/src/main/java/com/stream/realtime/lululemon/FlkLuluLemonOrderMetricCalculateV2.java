package com.stream.realtime.lululemon;

import com.stream.core.EnvironmentSettingUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.stream.realtime.lululemon.FlkLuluLemonOrderMetricCalculateV2
 * @Author zhou.han
 * @Date 2025/10/29 11:11
 * @description: stream and batch
 */
public class FlkLuluLemonOrderMetricCalculateV2 {

    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // tenv 参数
        tenv.getConfig().getConfiguration().setString("table.local-time-zone", "Asia/Shanghai");
        tenv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "30 s");
        tenv.getConfig().getConfiguration().setString("taskmanager.memory.task.off-heap.size", "1024mb");
        tenv.getConfig().getConfiguration().setString("taskmanager.memory.managed.size", "2gb");
        tenv.getConfig().getConfiguration().setString("table.exec.resource.default-parallelism", "1");
        tenv.getConfig().getConfiguration().setString("table.exec.window-agg.buffer-size-limit", "500000");
        tenv.getConfig().getConfiguration().setString("table.exec.mini-batch.enabled", "true");
        tenv.getConfig().getConfiguration().setString("table.exec.mini-batch.allow-latency", "5 s");
        tenv.getConfig().getConfiguration().setString("table.exec.mini-batch.size", "5000");

        //system 参数
        System.setProperty("XX:ReservedCodeCacheSize", "512m");



        String ddlKafkaOmsOrderInfo = "CREATE TABLE IF NOT EXISTS kafka_oms_order_dtl ( " +
                "    id BIGINT, " +
                "    order_id STRING, " +
                "    user_id STRING, " +
                "    user_name STRING, " +
                "    phone_number STRING, " +
                "    product_link STRING, " +
                "    product_id STRING, " +
                "    color STRING, " +
                "    size STRING, " +
                "    item_id STRING, " +
                "    material STRING, " +
                "    sale_num STRING, " +
                "    sale_amount STRING, " +
                "    total_amount STRING, " +
                "    product_name STRING, " +
                "    is_online_sales STRING, " +
                "    shipping_address STRING, " +
                "    recommendations_product_ids STRING, " +
                "    ds STRING, " +
                "    ts BIGINT, " +
                "    ts_ms AS CASE WHEN ts < 100000000000 THEN TO_TIMESTAMP_LTZ(ts * 1000, 3) ELSE TO_TIMESTAMP_LTZ(ts, 3) END, " +
                "    insert_time STRING, " +
                "    table_name STRING, " +
                "    op STRING, " +
                "    proc_time AS PROCTIME(), " +
                "    WATERMARK FOR ts_ms AS ts_ms - INTERVAL '5' SECOND " +
                ") WITH ( " +
                "    'connector' = 'kafka', " +
                "    'topic' = 'realtime_v3_order_info', " +
                "    'properties.bootstrap.servers' = 'cdh01:9092,cdh02:9092,cdh03:9092', " +
                "    'properties.group.id' = 'order-analysis-merged_v1', " +
                "    'scan.startup.mode' = 'earliest-offset', " +
                "    'format' = 'json', " +
                "    'json.fail-on-missing-field' = 'false', " +
                "    'json.ignore-parse-errors' = 'true' " +
                ");";

        String dorisSinkDDL = String.format(
                "CREATE TABLE IF NOT EXISTS report_lululemon_window_gmv_topN ( " +
                        "    ds DATE, " +
                        "    window_start STRING, " +
                        "    window_end STRING, " +
                        "    win_gmv DECIMAL(18,2), " +
                        "    win_gmv_ids STRING, " +
                        "    top5_product_ids STRING, " +
                        "    PRIMARY KEY (ds, window_start, window_end) NOT ENFORCED" +
                        ") WITH ( " +
                        "    'connector' = 'doris', " +
                        "    'fenodes' = '10.160.60.14:8030', " +
                        "    'table.identifier' = 'bigdata_realtime_report_v3.report_lululemon_window_gmv_topN', " +
                        "    'username' = 'root', " +
                        "    'password' = 'zh1028,./', " +
                        "    'sink.label-prefix' = 'lululemon_gmv_%d', " +
                        "    'sink.enable-2pc' = 'true', " +
                        "    'sink.properties.format' = 'json', " +
                        "    'sink.properties.read_json_by_line' = 'true' " +
                        ")",
                System.currentTimeMillis()
        );

        tenv.executeSql(ddlKafkaOmsOrderInfo);
        tenv.executeSql(dorisSinkDDL);

        String computeAllGMV = " "
                + "WITH base_t AS ( "
                + "    SELECT "
                + "        TRIM(REPLACE(product_id, '\\n', '')) AS clean_product_id, "
                + "        CAST(total_amount AS DECIMAL(18,2)) AS total_amount, "
                + "        ts_ms "
                + "    FROM kafka_oms_order_dtl "
                + "), "
                + "window_product_gmv AS ( "
                + "    SELECT "
                + "        window_start, "
                + "        window_end, "
                + "        clean_product_id, "
                + "        SUM(total_amount) AS product_gmv "
                + "    FROM TABLE( "
                + "        CUMULATE(TABLE base_t, DESCRIPTOR(ts_ms), INTERVAL '10' MINUTES, INTERVAL '1' DAY) "
                + "    ) "
                + "    GROUP BY window_start, window_end, clean_product_id "
                + "), "
                + "ranked_products AS ( "
                + "    SELECT "
                + "        window_start, "
                + "        window_end, "
                + "        clean_product_id, "
                + "        product_gmv, "
                + "        ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY product_gmv DESC) AS rn "
                + "    FROM window_product_gmv "
                + "), "
                + "win_agg AS ( "
                + "    SELECT "
                + "        window_start, "
                + "        window_end, "
                + "        CAST(DATE_FORMAT(window_start, 'yyyy-MM-dd') AS DATE) AS ds, "
                + "        SUM(product_gmv) AS win_gmv, "
                + "        LISTAGG(clean_product_id, ',') AS win_gmv_ids "
                + "    FROM window_product_gmv "
                + "    GROUP BY window_start, window_end "
                + ") "
                + "SELECT "
                + "    w.ds, "
                + "    DATE_FORMAT(w.window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start, "
                + "    DATE_FORMAT(w.window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end, "
                + "    CAST(w.win_gmv AS DECIMAL(18,2)) AS win_gmv, "
                + "    w.win_gmv_ids, "
                + "    LISTAGG(r.clean_product_id, ',') AS top5_product_ids "
                + "FROM win_agg w "
                + "LEFT JOIN ranked_products r "
                + "    ON w.window_start = r.window_start "
                + "    AND w.window_end = r.window_end "
                + "WHERE r.rn <= 5 "
                + "GROUP BY w.ds, w.window_start, w.window_end, w.win_gmv, w.win_gmv_ids;";



        tenv.executeSql("insert into report_lululemon_window_gmv_topN" + computeAllGMV);
//        tenv.executeSql(computeAllGMV).print();


    }

}
