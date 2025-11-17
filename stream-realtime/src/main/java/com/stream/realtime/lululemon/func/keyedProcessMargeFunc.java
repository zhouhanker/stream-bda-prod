package com.stream.realtime.lululemon.func;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.stream.realtime.lululemon.func.keyedProcessMargeFunc
 * @Author zhou.han
 * @Date 2025/11/17 18:53
 * @description:
 */
public class keyedProcessMargeFunc extends KeyedProcessFunction<String, JsonObject,JsonObject> {

    private MapState<String, JsonObject> deviceState;

    @Override
    public void open(Configuration parameters){
        deviceState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("deviceState", String.class, JsonObject.class)
        );
    }

    @Override
    public void processElement(JsonObject value, KeyedProcessFunction<String, JsonObject, JsonObject>.Context ctx, Collector<JsonObject> out) throws Exception {
        JsonObject deviceObj = value.getAsJsonObject("device");
        if (deviceObj == null) return;

        // device 的唯一 key，按 brand + plat
        String dedupKey = deviceObj.toString();   // 如果 JSON 完全相同则自动去重

        // 判断是否重复
        if (!deviceState.contains(dedupKey)) {
            deviceState.put(dedupKey, deviceObj);
        }

        // 输出聚合结果
        JsonObject output = new JsonObject();
        output.addProperty("user_id", value.get("user_id").getAsString());
        output.addProperty("ds", value.get("ds").getAsString());

        JsonArray deviceArray = new JsonArray();
        for (JsonObject dev : deviceState.values()) {
            deviceArray.add(dev);
        }
        output.add("devices", deviceArray);

        out.collect(output);
    }

    public static JsonObject computeDevices(JsonObject jsonObject){
        return  null;
    }

    public static JsonObject computeUserLogin(JsonObject jsonObject){
        return  null;
    }

    public static JsonObject computeUserSearch(JsonObject jsonObject){
        return  null;
    }

}
