package com.stream.realtime.lululemon.func;

import com.google.gson.JsonObject;
import com.stream.core.DateTimeUtils;
import com.stream.realtime.lululemon.service.TextAnalyzeService;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @Package com.stream.realtime.lululemon.func.MapSensitiveWordFunc
 * @Author zhou.han
 * @Date 2025/11/20 10:35
 * @description:
 */
public class MapSensitiveWordFunc extends RichMapFunction<JsonObject, JsonObject> {
    @Override
    public JsonObject map(JsonObject value) throws Exception {

        String userComment = "";
        if (value.has("user_comment") && !value.get("user_comment").isJsonNull()) {
            userComment = value.get("user_comment").getAsString();
        }

        long ts = 0L;
        if (value.has("ts") && !value.get("ts").isJsonNull()) {
            try {
                ts = value.get("ts").getAsLong();
            } catch (Exception e) {
                // ts 不是 long，比如字符串，需要降级转换
                try {
                    ts = Long.parseLong(value.get("ts").getAsString());
                } catch (Exception ex) {
                    ts = System.currentTimeMillis();
                }
            }
        } else {
            ts = System.currentTimeMillis();
        }

        String ds = DateTimeUtils.tsToDate(ts);

        if (userComment == null || userComment.trim().isEmpty()) {
            value.addProperty("sw_p0", "[]");
            value.addProperty("sw_p1", "[]");
            value.addProperty("ds", ds);
            return value;
        }

        TextAnalyzeService.AnalyzeResult analyzeResultP0 = TextAnalyzeService.analyzeP0(userComment);
        TextAnalyzeService.AnalyzeResult analyzeResultP1 = TextAnalyzeService.analyzeP1(userComment);

        value.addProperty("sw_p0", analyzeResultP0.sensitiveWords.toString());
        value.addProperty("sw_p1", analyzeResultP1.sensitiveWords.toString());
        value.addProperty("ds", ds);

        return value;
    }
}
