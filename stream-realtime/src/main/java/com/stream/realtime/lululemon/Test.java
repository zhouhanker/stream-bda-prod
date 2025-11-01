package com.stream.realtime.lululemon;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.stream.core.DateTimeUtils;

import java.util.Date;


/**
 * @Package com.stream.realtime.lululemon.Test
 * @Author zhou.han
 * @Date 2025/10/27 13:47
 * @description:
 */
public class Test {

    public static void main(String[] args) {

        String s = "{\"log_id\":\"d664dbd910f44ab29f428bb061726b55\",\"device\":{\"brand\":\"honor\",\"plat\":\"android\",\"platv\":\"10\",\"softv\":\"7.84.0\",\"uname\":\"\",\"userkey\":\"fc9d23c817271c8e\",\"device\":\"bkl-al00\"},\"gis\":{\"ip\":\"120.235.32.180\"},\"network\":{\"net\":\"wifi\"},\"opa\":\"pageinfo\",\"log_type\":\"product_detail\",\"ts\":1761944836,\"product_id\":\"10109988265079\",\"order_id\":\"710c4b1920664c7fb959d76eaa5a6ba9\",\"user_id\":\"d717278c-9d31-48ac-bdb6-148e8254f048\"}";
        JsonObject jsonObject = JsonParser.parseString(s).getAsJsonObject();
        System.err.println(jsonObject);
        long ts = jsonObject.get("ts").getAsLong();
        System.err.println(ts);
        System.err.println(DateTimeUtils.tsToDate(ts));


    }
}
