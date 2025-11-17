package com.stream.realtime.lululemon.func;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.stream.core.DateTimeUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Package com.stream.realtime.lululemon.func.MapConvertLogOriginAndGetDsTimeFunc
 * @Author zhou.han
 * @Date 2025/11/17 18:34
 * @description:
 */
public class MapConvertLogOriginAndGetDsTimeFunc implements MapFunction<String, JsonObject> {
    @Override
    public JsonObject map(String value) throws Exception {
        JsonObject valueJson = JsonParser.parseString(value).getAsJsonObject();
        long ts = valueJson.get("ts").getAsLong();
        String userId = valueJson.get("user_id").getAsString();
        String ds = DateTimeUtils.tsToDate(ts);
        valueJson.addProperty("ds",ds);
        valueJson.addProperty("key_column",userId+"_"+ds);
        return valueJson;
    }
}
