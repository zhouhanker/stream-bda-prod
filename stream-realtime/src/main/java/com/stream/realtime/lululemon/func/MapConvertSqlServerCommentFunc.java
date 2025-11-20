package com.stream.realtime.lululemon.func;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Package com.stream.realtime.lululemon.func.MapConvertSqlServerCommentFunc
 * @Author zhou.han
 * @Date 2025/11/20 10:26
 * @description:
 */
public class MapConvertSqlServerCommentFunc implements MapFunction<String, JsonObject> {
    @Override
    public JsonObject map(String s) throws Exception {
        JsonObject value = JsonParser.parseString(s).getAsJsonObject();
        if (value.has("after")){
            return value.get("after").getAsJsonObject();
        }
        return null;
    }
}
