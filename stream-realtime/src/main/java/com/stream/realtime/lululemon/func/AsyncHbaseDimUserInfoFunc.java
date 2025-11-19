package com.stream.realtime.lululemon.func;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.JsonObject;
import com.stream.core.HbaseUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @description: 异步补全 dim_user_info_v3
 */
public class AsyncHbaseDimUserInfoFunc extends RichAsyncFunction<JsonObject, JsonObject> {

    private transient Connection hbaseConn;
    private transient Table hbaseDimUserInfo;

    // 本地缓存 rowKey → JsonObject（uname, phone, gender...）
    private transient Cache<String, JsonObject> cache;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = new HbaseUtils("cdh01:2181,cdh02:2181,cdh03:2181").getConnection();
        hbaseDimUserInfo = hbaseConn.getTable(TableName.valueOf("realtime_v3:dim_user_info_v3"));

        cache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(30, TimeUnit.MINUTES)
                .build();

        super.open(parameters);
    }

    @Override
    public void asyncInvoke(JsonObject input, ResultFuture<JsonObject> resultFuture) {

        if (!input.has("user_id")) {
            resultFuture.complete(Collections.singleton(input));
            return;
        }

        String userId = input.get("user_id").getAsString();

        // HBase rowkey = md5(user_id)
        String rowKey = MD5Hash.getMD5AsHex(userId.getBytes(StandardCharsets.UTF_8));

        // 1. 优先查缓存
        JsonObject cached = cache.getIfPresent(rowKey);
        if (cached != null) {
            enrich(input, cached);
            resultFuture.complete(Collections.singleton(input));
            return;
        }

        // 2. 异步查询 HBase
        CompletableFuture
                .supplyAsync(() -> queryFromHbase(rowKey))
                .thenAccept(dimJson -> {
                    if (dimJson != null) {
                        cache.put(rowKey, dimJson);
                        enrich(input, dimJson);
                    }
                    resultFuture.complete(Collections.singleton(input));
                });

    }

    /**
     * 查询 HBase 维度数据
     */
    private JsonObject queryFromHbase(String rowKey) {

        try {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = hbaseDimUserInfo.get(get);

            if (result == null || result.isEmpty()) {
                return null;
            }

            JsonObject dim = new JsonObject();

            dim.addProperty("uname", stripOuterQuotes(getStr(result, "uname")));
            dim.addProperty("phone_num", stripOuterQuotes(getStr(result, "phone_num")));
            dim.addProperty("gender", stripOuterQuotes(getStr(result, "gender")));
            dim.addProperty("birthday", stripOuterQuotes(getStr(result, "birthday")));

            return dim;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /** 从 HBase 获取单个字段 */
    private String getStr(Result result, String col) {
        byte[] v = result.getValue(Bytes.toBytes("info"), Bytes.toBytes(col));
        return v == null ? "" : Bytes.toString(v);
    }

    /**
     * 去掉字段外层的引号，例如："李冉" → 李冉
     */
    private String stripOuterQuotes(String s) {
        if (s == null) return "";
        s = s.trim();
        if (s.startsWith("\"") && s.endsWith("\"") && s.length() >= 2) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    /**
     * 补全维度到 input Json 中
     */
    private void enrich(JsonObject input, JsonObject dim) {

        if (!input.has("user_info")) {
            input.add("user_info", new JsonObject());
        }

        JsonObject target = input.getAsJsonObject("user_info");

        if (dim.has("uname"))
            target.addProperty("uname", dim.get("uname").getAsString());

        if (dim.has("phone_num"))
            target.addProperty("phone_num", dim.get("phone_num").getAsString());

        if (dim.has("gender"))
            target.addProperty("gender", dim.get("gender").getAsString());

        if (dim.has("birthday"))
            target.addProperty("birthday", dim.get("birthday").getAsString());
    }


    @Override
    public void timeout(JsonObject input, ResultFuture<JsonObject> resultFuture) {
        // 查询超时，直接输出原始数据
        resultFuture.complete(Collections.singleton(input));
    }

    @Override
    public void close() throws Exception {
        try {
            if (hbaseDimUserInfo != null) hbaseDimUserInfo.close();
            if (hbaseConn != null) hbaseConn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        super.close();
    }
}
