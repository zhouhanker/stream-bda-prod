package com.stream.realtime.lululemon.func;

import com.google.gson.JsonObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.stream.realtime.lululemon.func.coProcessUserInfoAndCommentJoinFunc
 * @Author zhou.han
 * @Date 2025/11/20 13:14
 * @description:
 */
public class coProcessUserInfoAndCommentJoinFunc extends CoProcessFunction<JsonObject,JsonObject,JsonObject> {

    private ValueState<JsonObject> userInfoState;
    private ValueState<JsonObject> commentState;


    @Override
    public void open(Configuration parameters) {

        userInfoState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("userInfoState", JsonObject.class)
        );

        commentState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("commentState", JsonObject.class)
        );
    }
    @Override
    public void processElement1(JsonObject jsonObject, CoProcessFunction<JsonObject, JsonObject, JsonObject>.Context context, Collector<JsonObject> collector) throws Exception {
        userInfoState.update(jsonObject);

        JsonObject comment = commentState.value();
        if (comment != null) {
            collector.collect(join(jsonObject, comment));
        }
    }

    @Override
    public void processElement2(JsonObject jsonObject, CoProcessFunction<JsonObject, JsonObject, JsonObject>.Context context, Collector<JsonObject> collector) throws Exception {
        commentState.update(jsonObject);

        JsonObject userInfo = userInfoState.value();
        if (userInfo != null) {
            collector.collect(join(userInfo, jsonObject));
        }
    }

    private JsonObject join(JsonObject userInfo, JsonObject comment) {

        JsonObject result = new JsonObject();

        // user_id、ds 是 join key
        result.addProperty("user_id", userInfo.get("user_id").getAsString());
        result.addProperty("ds", userInfo.get("ds").getAsString());

        // --- 用户属性 ---
        for (String key : userInfo.keySet()) {
            if (!"user_id".equals(key) && !"ds".equals(key))
                result.add(key, userInfo.get(key));
        }

        // --- 评论敏感词 + 订单明细 ---
        for (String key : comment.keySet()) {
            if (!"user_id".equals(key) && !"ds".equals(key))
                result.add(key, comment.get(key));
        }

        return result;
    }
}
