package com.stream.realtime.lululemon.func;

import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.stream.realtime.lululemon.func.MapDevice2DorisColumnFunc
 * @Author zhou.han
 * @Date 2025/11/5 14:45
 * @description:
 */
public class MapDevice2DorisColumnFunc implements MapFunction<JsonObject, String> {

    private static final DateTimeFormatter FROM = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter TO   = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    @Override
    public String map(JsonObject in) throws Exception {
        String date = in.has("date") ? in.get("date").getAsString() : null;
        String pt = (date == null || date.isEmpty())
                ? null
                : LocalDate.parse(date, FROM).format(TO);

        String os    = in.has("os")    ? in.get("os").getAsString()    : null;
        String brand = in.has("brand") ? in.get("brand").getAsString() : null;
        String platv = in.has("platv") ? in.get("platv").getAsString() : null;
        Long count   = in.has("count") ? in.get("count").getAsLong()   : null;
        String type  = in.has("type")  ? in.get("type").getAsString()  : null;

        JsonObject out = new JsonObject();
        if (pt != null)    out.addProperty("pt", pt);
        if (os != null)    out.addProperty("os", os);
        if (brand != null) out.addProperty("brand", brand);
        if (platv != null) out.addProperty("platv", platv);
        if (count != null) out.addProperty("count", count);
        if (type != null)  out.addProperty("type", type);

        return out.toString();
    }
}
