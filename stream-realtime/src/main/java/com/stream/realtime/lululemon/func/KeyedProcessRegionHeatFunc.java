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
 * @Package com.stream.realtime.lululemon.func.KeyedProcessRegionHeatFunc
 * @Author zhou.han
 * @Date 2025/11/3 14:51
 * @description:
 */
public class KeyedProcessRegionHeatFunc extends KeyedProcessFunction<String, JsonObject, JsonObject> {

    private transient ValueState<Long> countState;
    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) {
        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("regionCountState", Long.class));
        timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("regionTimerState", Long.class));
    }

    @Override
    public void processElement(JsonObject value, Context ctx, Collector<JsonObject> out) throws Exception {
        Long current = countState.value();
        if (current == null) current = 0L;
        current += 1;
        countState.update(current);

        long ts = value.has("ts") ? value.get("ts").getAsLong() : System.currentTimeMillis();
        String date = DateTimeUtils.tsToDate(ts);

        String regionRaw = value.has("region") ? value.get("region").getAsString() : null;
        String ip = null;
        if (value.has("gis") && value.getAsJsonObject("gis").has("ip")) {
            ip = value.getAsJsonObject("gis").get("ip").getAsString();
        }

        String province = null, city = null, district = null;
        boolean abnormal = false;

        try {
            if (regionRaw != null && regionRaw.contains("|")) {
                String[] parts = regionRaw.split("\\|");
                if (parts.length >= 2) province = safeVal(parts[1]);
                if (parts.length >= 3) city = safeVal(parts[2]);
                if (parts.length >= 4) district = safeVal(parts[3]);

                if ("0".equals(province) || "0".equals(city) || "0".equals(district)) {
                    province = null;
                    city = null;
                    district = null;
                    abnormal = true;
                }
            } else {
                abnormal = true;
            }
        } catch (Exception e) {
            abnormal = true;
        }

        String region = (province != null ? province : "null") + "|" +
                (city != null ? city : "null") + "|" +
                (district != null ? district : "null");

        String currentDay = LocalDate.now(ZoneId.of("Asia/Shanghai"))
                .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        boolean isToday = currentDay.equals(date);

        JsonObject result = buildOutput(date, region, province, city, district, current, abnormal, ip, regionRaw, isToday ? "periodic" : "history");
        if (!isToday) {
            out.collect(result);
            return;
        }

        long currentTs = ctx.timerService().currentProcessingTime();
        Long nextTimer = timerState.value();
        if (nextTimer == null || currentTs >= nextTimer) {
            long triggerTs = currentTs + 5 * 60 * 1000L;
            ctx.timerService().registerProcessingTimeTimer(triggerTs);
            timerState.update(triggerTs);
        }

        out.collect(result);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JsonObject> out) throws Exception {
        Long current = countState.value();
        if (current == null) return;

        String[] keyParts = ctx.getCurrentKey().split("\\|");
        String date = keyParts.length >= 1 ? keyParts[0] : "unknown";
        String region = keyParts.length >= 2 ? keyParts[1] : "unknown";

        JsonObject result = new JsonObject();
        result.addProperty("date", date);
        result.addProperty("region", region);
        result.addProperty("count", current);
        result.addProperty("timestamp", System.currentTimeMillis());
        result.addProperty("type", "periodic");
        out.collect(result);
    }

    private JsonObject buildOutput(String date, String region, String province, String city, String district,
                                   Long count, boolean abnormal, String ip, String regionRaw, String type) {
        JsonObject result = new JsonObject();
        result.addProperty("date", date);
        result.addProperty("region", region);
        result.addProperty("province", province);
        result.addProperty("city", city);
        result.addProperty("district", district);
        result.addProperty("count", count);
        result.addProperty("abnormal", abnormal);
        result.addProperty("region_raw", regionRaw);
        result.addProperty("type", type);

        if (province == null || province.equals("null")) {
            result.addProperty("ip", ip);
        } else {
            result.addProperty("ip", (String) null);
        }

        return result;
    }

    private String safeVal(String s) {
        if (s == null) return null;
        s = s.trim();
        return s.isEmpty() || "0".equals(s) ? null : s;
    }
}
