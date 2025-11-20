package com.stream.realtime.lululemon.func;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.stream.core.DateTimeUtils;
import com.stream.core.IPUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;

/**
 * @Package com.stream.realtime.lululemon.func.MapFixUserInfoData2DorisFunc
 * @Author zhou.han
 * @Date 2025/11/20 16:52
 * @description:
 */
public class MapFixUserInfoData2DorisFunc implements MapFunction<JsonObject, JsonObject> {
    @Override
    public JsonObject map(JsonObject value) throws Exception {
        String ds = value.get("ds").getAsString();
        String pt = DateTimeUtils.ds2DorisPt(ds);

        JsonArray gis = value.get("gis").getAsJsonArray();
        JsonArray resultGis = new JsonArray();

        for (JsonElement element : gis) {
            JsonObject ips = element.getAsJsonObject();

            // 正确获取 IP
            String ip = ips.get("ip").getAsString();

            String region = IPUtils.ip2Region(ip);

            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("ip", ip);
            jsonObject.addProperty("region", region);

            resultGis.add(jsonObject);
        }

        value.remove("gis");
        value.remove("ds");

        // 正确添加 JSON 数组，而不是字符串
        value.add("gis", resultGis);

        value.addProperty("pt", pt);

        return value;
    }

}
