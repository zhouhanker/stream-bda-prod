package com.stream.realtime.lululemon.func;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.stream.realtime.lululemon.func.FlatMapKeyWordsArrFunc
 * @Author zhou.han
 * @Date 2025/11/1 16:43
 * @description: 打平关键字数组
 */
public class FlatMapKeyWordsArrFunc extends RichFlatMapFunction<JsonObject,JsonObject> {
    @Override
    public void flatMap(JsonObject jsonObject, Collector<JsonObject> collector){

        if (jsonObject.has("keywords") && jsonObject.get("keywords").isJsonArray()){
            JsonArray arr = jsonObject.getAsJsonArray("keywords");
            for (int i = 0; i < arr.size(); i++) {
                JsonObject copy = jsonObject.deepCopy();
                copy.addProperty("keyword", arr.get(i).getAsString());
                collector.collect(copy);
            }
        }
    }
}
