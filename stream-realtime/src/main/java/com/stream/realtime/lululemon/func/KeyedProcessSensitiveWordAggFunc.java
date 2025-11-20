package com.stream.realtime.lululemon.func;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * @Package com.stream.realtime.lululemon.func.KeyedProcessSensitiveWordAggFunc
 * @Author zhou.han
 * @Date 2025/11/20 11:17
 * @description:
 */
public class KeyedProcessSensitiveWordAggFunc extends KeyedProcessFunction<String, JsonObject,JsonObject> {

    private ValueState<String> userIdState;
    private ValueState<String> dsState;

    // 存 order_id + sw_p0 + sw_p1
    private ListState<JsonObject> orderListState;

    @Override
    public void open(Configuration parameters){

        userIdState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("userIdState", String.class));

        dsState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("dsState", String.class));

        orderListState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("orderListState", JsonObject.class));
    }

    @Override
    public void processElement(JsonObject value, Context ctx, Collector<JsonObject> out) throws Exception {

        String userId = value.get("user_id").getAsString();
        String ds = value.get("ds").getAsString();

        userIdState.update(userId);
        dsState.update(ds);

        // 将本条订单明细存入状态
        JsonObject orderItem = new JsonObject();
        orderItem.addProperty("order_id", value.get("order_id").getAsString());

        orderItem.add("sw_p0", toJsonArray(value.get("sw_p0").getAsString()));
        orderItem.add("sw_p1", toJsonArray(value.get("sw_p1").getAsString()));

        orderListState.add(orderItem);

        // 延迟 2 秒等待同 user_id+ds 的其他数据
        ctx.timerService().registerProcessingTimeTimer(
                ctx.timerService().currentProcessingTime() + 2000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JsonObject> out) throws Exception {

        String userId = userIdState.value();
        String ds = dsState.value();

        JsonArray ordersArray = new JsonArray();
        Set<String> p0Set = new HashSet<>();
        Set<String> p1Set = new HashSet<>();

        for (JsonObject orderObj : orderListState.get()) {

            // 添加订单明细到 orders 数组
            ordersArray.add(orderObj);

            // 同时聚合全量词语
            addAllToSet(orderObj.get("sw_p0").getAsJsonArray(), p0Set);
            addAllToSet(orderObj.get("sw_p1").getAsJsonArray(), p1Set);
        }

        // 构建最终输出
        JsonObject result = new JsonObject();
        result.addProperty("user_id", userId);
        result.addProperty("ds", ds);

        result.add("orders", ordersArray);

        // 日级去重敏感词
        result.add("sw_p0", toJsonArray(p0Set));
        result.add("sw_p1", toJsonArray(p1Set));

        out.collect(result);

        // 清理状态
        orderListState.clear();
    }



    private JsonArray toJsonArray(String str) {
        JsonArray arr = new JsonArray();
        if (str == null || str.length() <= 2) return arr;

        str = str.substring(1, str.length() - 1); // 去掉 [ ]
        for (String s : str.split(",")) {
            s = s.trim();
            if (!s.isEmpty()) arr.add(s);
        }
        return arr;
    }

    private JsonArray toJsonArray(Set<String> set) {
        JsonArray arr = new JsonArray();
        for (String s : set) arr.add(s);
        return arr;
    }

    private void addAllToSet(JsonArray arr, Set<String> set) {
        for (int i = 0; i < arr.size(); i++) {
            set.add(arr.get(i).getAsString());
        }
    }
}
