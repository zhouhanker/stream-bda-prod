package com.stream.realtime.lululemon.func;

import com.google.gson.JsonObject;
import com.stream.core.IPUtils;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @Package com.stream.realtime.lululemon.func.MapGisMessage2Region
 * @Author zhou.han
 * @Date 2025/11/3 08:22
 * @description: ip 2 region
 */
public class MapGisMessage2Region extends RichMapFunction<JsonObject,JsonObject> {
    @Override
    public JsonObject map(JsonObject data) throws Exception {
        String ip = data.getAsJsonObject("gis").get("ip").getAsString();
        String region = IPUtils.ip2Region(ip);
        data.addProperty("region",region);
        return data;
    }

    @Override
    public void close() throws Exception {
        super.close();
        IPUtils.close();
    }
}
