package com.stream.realtime.lululemon.func;

import com.google.gson.JsonObject;
import com.stream.core.DateTimeUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.stream.realtime.lululemon.func.KeyedProcessDeviceStatsFunc
 * @Author zhou.han
 * @Date 2025/11/5 09:58
 * @description:
 */
public class KeyedProcessDeviceStatsFunc extends KeyedProcessFunction<String, JsonObject,JsonObject> {

    private transient ValueState<Long> countState;
    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters){
        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("deviceCountState", Long.class));
        timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("deviceTimerState", Long.class));
    }

    @Override
    public void processElement(JsonObject value, KeyedProcessFunction<String, JsonObject, JsonObject>.Context ctx, Collector<JsonObject> out) throws Exception {
        Long current = countState.value();
        if (current == null) current = 0L;
        current += 1;
        countState.update(current);

        long ts = value.has("ts") ? value.get("ts").getAsLong() : System.currentTimeMillis();
        String date = DateTimeUtils.tsToDate(ts);

        JsonObject dev = value.getAsJsonObject("device");
        String os = dev.has("plat") ? dev.get("plat").getAsString().toLowerCase() : "unknown";
        String brand = dev.has("brand") ? dev.get("brand").getAsString().toLowerCase() : "unknown";
        String platv = dev.has("platv") ? dev.get("platv").getAsString().toLowerCase() : "unk";

        // 判断是否当天
        String currentDay = LocalDate.now(ZoneId.of("Asia/Shanghai"))
                .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        boolean isToday = currentDay.equals(date);

        // 输出
        JsonObject result = new JsonObject();
        result.addProperty("date", date);
        result.addProperty("os", os);
        result.addProperty("brand", brand);
        result.addProperty("platv", platv);
        result.addProperty("count", current);
        result.addProperty("type", isToday ? "periodic" : "history");
        out.collect(result);

        // 当天注册 5min 定时器
        if (isToday) {
            long currentTs = ctx.timerService().currentProcessingTime();
            Long nextTimer = timerState.value();
            if (nextTimer == null || currentTs >= nextTimer) {
                long triggerTs = currentTs + 5 * 60 * 1000L;
                ctx.timerService().registerProcessingTimeTimer(triggerTs);
                timerState.update(triggerTs);
            }
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, JsonObject, JsonObject>.OnTimerContext ctx, Collector<JsonObject> out) throws Exception {
        Long current = countState.value();
        if (current == null) return;

        String[] keyParts = ctx.getCurrentKey().split("\\|");
        String date = keyParts.length >= 1 ? keyParts[0] : "unknown";
        String os = keyParts.length >= 2 ? keyParts[1] : "unknown";
        String brand = keyParts.length >= 3 ? keyParts[2] : "unknown";
        String platv = keyParts.length >= 4 ? keyParts[3] : "unk";

        JsonObject result = new JsonObject();
        result.addProperty("date", date);
        result.addProperty("os", os);
        result.addProperty("brand", brand);
        result.addProperty("platv", platv);
        result.addProperty("count", current);
        result.addProperty("timestamp", System.currentTimeMillis());
        result.addProperty("type", "periodic");
        out.collect(result);
    }
}
