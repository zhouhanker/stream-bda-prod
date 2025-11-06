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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Package com.stream.realtime.lululemon.func.KeyedProcessSearchTOPNFunc
 * @Author zhou.han
 * @Date 2025/11/1 16:21
 * @description:
 */
public class KeyedProcessSearchTOPNFunc extends KeyedProcessFunction<String, JsonObject,String> {

    private transient ValueState<Long> countState;
    private transient MapState<String, Long> topMap;
    private transient ValueState<Long> lastTimer;

    @Override
    public void open(Configuration parameters){
        ValueStateDescriptor<Long> cntDesc = new ValueStateDescriptor<>("searchCnt", Types.LONG);
        cntDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(2)).build());
        countState = getRuntimeContext().getState(cntDesc);

        MapStateDescriptor<String, Long> mapDesc =
                new MapStateDescriptor<>("topMap", Types.STRING, Types.LONG);
        mapDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
        topMap = getRuntimeContext().getMapState(mapDesc);

        ValueStateDescriptor<Long> timerDesc = new ValueStateDescriptor<>("lastTimer", Types.LONG);
        lastTimer = getRuntimeContext().getState(timerDesc);
    }

    @Override
    public void processElement(JsonObject value, KeyedProcessFunction<String, JsonObject, String>.Context ctx, Collector<String> out) throws Exception {
        String key = ctx.getCurrentKey();
        String[] parts = key.split("\\|", 2);
        String day = parts[0];
        String word = parts.length > 1 ? parts[1] : "";

        long c = countState.value() == null ? 0L : countState.value();
        countState.update(c + 1);

        if (topMap.contains(word)) {
            topMap.put(word, topMap.get(word) + 1);
        } else {
            topMap.put(word, 1L);
        }

        long now = ctx.timerService().currentProcessingTime();
        long next = (now / (5 * 60_000) + 1) * 5 * 60_000;
        Long last = lastTimer.value();
        if (last == null || next > last) {
            ctx.timerService().registerProcessingTimeTimer(next);
            lastTimer.update(next);
        }
    }

    @Override
    public void onTimer(long ts, KeyedProcessFunction<String, JsonObject, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        Map<String, Long> snapshot = new HashMap<>();
        for (Map.Entry<String, Long> e : topMap.entries()) {
            snapshot.put(e.getKey(), e.getValue());
        }

        List<Map.Entry<String, Long>> top10 = snapshot.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                .limit(10)
                .collect(Collectors.toList());

        java.time.format.DateTimeFormatter formatter = java.time.format
                .DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(java.time.ZoneId.systemDefault());
        String formattedTime = formatter.format(java.time.Instant.now());

        String day = ctx.getCurrentKey().split("\\|")[0];
        JsonObject result = new JsonObject();
        result.addProperty("pt", DateTimeUtils.ds2DorisPt(day));
        JsonArray arr = new JsonArray();
        for (Map.Entry<String, Long> e : top10) {
            JsonObject item = new JsonObject();
            item.addProperty("word", e.getKey());
            item.addProperty("cnt", e.getValue());
            arr.add(item);
        }
        result.add("search_item", arr);
        result.addProperty("emit_time", formattedTime);
        out.collect(String.valueOf(result));
    }


}
