package com.stream.realtime.lululemon.func;

import com.google.gson.JsonObject;
import com.stream.core.DateTimeUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Package com.stream.realtime.lululemon.func.MapRegionData2DorisColumnFunc
 * @Author zhou.han
 * @Date 2025/11/6 15:10
 * @description:
 */
public class MapRegionData2DorisColumnFunc implements MapFunction<JsonObject,String> {
    @Override
    public String map(JsonObject jsonObject){
        String pt = jsonObject.has("pt")? jsonObject.get("pt").getAsString(): null;
        String dorisPt = DateTimeUtils.ds2DorisPt(pt);
        jsonObject.addProperty("pt",dorisPt);
        return jsonObject.toString();
    }
}
