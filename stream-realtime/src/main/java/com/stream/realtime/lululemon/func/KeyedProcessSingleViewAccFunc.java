package com.stream.realtime.lululemon.func;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.stream.core.DateTimeUtils;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * @Package com.stream.realtime.lululemon.func.KeyedProcessSingleViewAccFunc
 * @Author zhou.han
 * @Date 2025/11/1 14:27
 * @description: 每个页面的用户访问
 */
public class KeyedProcessSingleViewAccFunc extends KeyedProcessFunction<String, JsonObject,String> {

    private transient ValueState<Long> pvState;
    private transient MapState<String, Boolean> userMap;
    private transient ValueState<Long> lastTimer;


    @Override
    public void open(Configuration parameters){
        ValueStateDescriptor<Long> pvDesc = new ValueStateDescriptor<>("pvState", Types.LONG);
        pvDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(2)).build());
        pvState = getRuntimeContext().getState(pvDesc);

        MapStateDescriptor<String, Boolean> userDesc =
                new MapStateDescriptor<>("userMap", Types.STRING, Types.BOOLEAN);
        userDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
        userMap = getRuntimeContext().getMapState(userDesc);

        ValueStateDescriptor<Long> timerDesc = new ValueStateDescriptor<>("timerState", Types.LONG);
        lastTimer = getRuntimeContext().getState(timerDesc);
    }

    @Override
    public void processElement(JsonObject value, KeyedProcessFunction<String, JsonObject, String>.Context ctx, Collector<String> collector) throws Exception {

        long current = pvState.value() == null ? 0L : pvState.value();
        pvState.update(current + 1);

        if (value.has("user_id")) {
            String uid = value.get("user_id").getAsString();
            if (uid != null && !uid.isEmpty()) {
                userMap.put(uid, true);
            }
        }

        long now = ctx.timerService().currentProcessingTime();
        long nextTrigger = (now / (5 * 60_000) + 1) * 5 * 60_000;
        Long last = lastTimer.value();

        if (last == null || nextTrigger > last) {
            ctx.timerService().registerProcessingTimeTimer(nextTrigger);
            lastTimer.update(nextTrigger);
        }


    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, JsonObject, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        String key = ctx.getCurrentKey();
        long pv = pvState.value() == null ? 0L : pvState.value();
        List<String> users = new ArrayList<>();

        for (String uid : userMap.keys()) {
            users.add(uid);
        }

        String[] parts = key.split("\\|", 2);
        String day = parts[0];
        String page = parts.length > 1 ? parts[1] : "unknown";

        java.time.format.DateTimeFormatter formatter = java.time.format
                 .DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(java.time.ZoneId.systemDefault());
        String formattedTime = formatter.format(java.time.Instant.now());

        JsonObject result = new JsonObject();
        result.addProperty("pt", DateTimeUtils.ds2DorisPt(day));
        result.addProperty("page", page);
        result.addProperty("pv", pv);
        JsonArray arr = new JsonArray();
        users.forEach(arr::add);
        result.add("user_ids", arr);
        result.addProperty("emit_time", formattedTime);

        out.collect(String.valueOf(result));
    }




}
