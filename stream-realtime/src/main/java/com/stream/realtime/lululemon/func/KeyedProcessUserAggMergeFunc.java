package com.stream.realtime.lululemon.func;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.stream.core.DateTimeUtils;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @Package com.stream.realtime.lululemon.func.KeyedProcessUserAggMergeFunc
 * @Author zhou.han
 * @Date 2025/11/19 09:40
 * @description: 用户按 (user_id, ds) 维度聚合，历史天单次输出、当天每3min更新
 */
public class KeyedProcessUserAggMergeFunc extends KeyedProcessFunction<String, JsonObject,JsonObject> {

    /** 聚合后的 JSON state */
    private transient ValueState<JsonObject> aggState;
    /** 历史天是否已经输出过一次的标记 */
    private transient ValueState<Boolean> historyOutputOnceFlag;
    /** 今日 3min 输出节流：下一次触发时间戳（processingTime） */
    private transient ValueState<Long> nextFireTsState;

    private static final DateTimeFormatter DS_FMT = DateTimeFormatter.BASIC_ISO_DATE;
    private static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");

    @Override
    public void open(Configuration parameters) {

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.days(7))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<JsonObject> aggDesc =
                new ValueStateDescriptor<>("user_daily_json", JsonObject.class);
        aggDesc.enableTimeToLive(ttlConfig);
        aggState = getRuntimeContext().getState(aggDesc);

        ValueStateDescriptor<Boolean> flagDesc =
                new ValueStateDescriptor<>("history_done_flag", Boolean.class);
        flagDesc.enableTimeToLive(ttlConfig);
        historyOutputOnceFlag = getRuntimeContext().getState(flagDesc);

        ValueStateDescriptor<Long> nextFireDesc =
                new ValueStateDescriptor<>("today_next_fire_ts", Long.class);
        nextFireDesc.enableTimeToLive(ttlConfig);
        nextFireTsState = getRuntimeContext().getState(nextFireDesc);
    }

    @Override
    public void processElement(
            JsonObject input,
            KeyedProcessFunction<String, JsonObject, JsonObject>.Context ctx,
            Collector<JsonObject> out) throws Exception {

        if (!input.has("user_id") || !input.has("ds") || !input.has("ts")) {
            return;
        }

        String ds = input.get("ds").getAsString();

        // 当前自然日（动态计算，避免跨天问题）
        String todayDs = LocalDate.now(ZONE).format(DS_FMT);

        // 当前 processing time（用于定时器触发）
        long nowProcTime = ctx.timerService().currentProcessingTime();

        // 1. 读写 state，做 Json 聚合
        JsonObject current = aggState.value();
        if (current == null) {
            current = buildEmptyAgg(input);
        }
        JsonObject inc = convertLogToAggJson(input);
        JsonAggUtils.merge(current, inc);
        aggState.update(current);

        // 2. 历史天逻辑：只输出一次
        if (ds.compareTo(todayDs) < 0) {
            // 只给历史天注册一次 timer，这里用 processing-time，延迟 1s 输出
            if (historyOutputOnceFlag.value() == null) {
                ctx.timerService().registerProcessingTimeTimer(nowProcTime + 1000L);
                historyOutputOnceFlag.update(true);
            }
        } else {
            // 3. 今天：按 3min 更新一次（processing-time）
            Long nextFireTs = nextFireTsState.value();
            if (nextFireTs == null || nowProcTime >= nextFireTs) {
                long fireTs = nowProcTime + 3 * 60 * 1000L;
                ctx.timerService().registerProcessingTimeTimer(fireTs);
                nextFireTsState.update(fireTs);
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JsonObject> out) throws Exception {
        JsonObject result = aggState.value();
        if (result == null) {
            return;
        }

        out.collect(result);

        // 历史天可以在输出一次后清理 state，节省空间
        String ds = result.get("ds").getAsString();
        String todayDs = LocalDate.now(ZONE).format(DS_FMT);
        if (ds.compareTo(todayDs) < 0) {
            aggState.clear();
            historyOutputOnceFlag.clear();
            nextFireTsState.clear();
        }
        // 对当天数据，不清理，继续滚动累加
    }

    /** 构造聚合结构 */
    private JsonObject buildEmptyAgg(JsonObject input) {
        JsonObject obj = new JsonObject();
        obj.addProperty("user_id", input.get("user_id").getAsString());
        obj.addProperty("ds", input.get("ds").getAsString());
        obj.add("login_time", new JsonArray());
        obj.add("device_info", new JsonArray());
        obj.add("search_info", new JsonArray());
        obj.add("gis", new JsonArray());
        return obj;
    }

    /** 当前日志 -> 只包含本条内容的增量聚合 JSON */
    private JsonObject convertLogToAggJson(JsonObject input) {
        JsonObject agg = buildEmptyAgg(input);

        // login_time
        JsonArray lt = agg.getAsJsonArray("login_time");
        String tsStr = input.get("ts").getAsString();
        long ts = DateTimeUtils.normalizeTs(tsStr);
        String timeStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(ts));
        lt.add(timeStr);

        // device_info：保留为一个个 JSON
        if (input.has("device")) {
            JsonArray arr = agg.getAsJsonArray("device_info");
            arr.add(input.get("device").deepCopy());
        }

        // search_info：keywords 数组去重
        if (input.has("keywords")) {
            JsonArray arr = agg.getAsJsonArray("search_info");
            input.getAsJsonArray("keywords").forEach(arr::add);
        }

        // gis：每条是一个 JSON
        if (input.has("gis")) {
            JsonArray arr = agg.getAsJsonArray("gis");
            arr.add(input.get("gis").deepCopy());
        }

        return agg;
    }

}


/**
 * Json 聚合工具：负责数组去重、device_info JSON 去重等
 */
class JsonAggUtils {

    public static void merge(JsonObject base, JsonObject add) {
        mergeArray(base, add, "login_time", false);
        mergeArray(base, add, "search_info", false);
        mergeArray(base, add, "gis", false);
        // device_info 是 JSON，比较时用 equals 做整体去重
        mergeArray(base, add, "device_info", true);
    }

    private static void mergeArray(JsonObject base, JsonObject add, String field, boolean jsonEquality) {
        if (!add.has(field)) return;

        JsonArray baseArr = base.getAsJsonArray(field);
        JsonArray addArr = add.getAsJsonArray(field);

        for (JsonElement e : addArr) {
            if (!contains(baseArr, e, jsonEquality)) {
                baseArr.add(e);
            }
        }
    }

    private static boolean contains(JsonArray arr, JsonElement target, boolean jsonEquality) {
        for (JsonElement e : arr) {
            if (jsonEquality) {
                if (e.equals(target)) return true;
            } else {
                if (e.toString().equals(target.toString())) return true;
            }
        }
        return false;
    }
}
