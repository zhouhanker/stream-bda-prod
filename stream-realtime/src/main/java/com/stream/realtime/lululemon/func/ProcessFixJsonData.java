package com.stream.realtime.lululemon.func;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.Data;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Package com.stream.realtime.lululemon.func.ProcessFixJsonData
 * @Author zhou.han
 * @Date 2025/10/24 19:00
 * @description:
 */
public class ProcessFixJsonData extends ProcessFunction<String, JsonObject> {

    private static final Logger logger = LoggerFactory.getLogger(ProcessFunction.class);


    OutputTag<String> ERROR_PARSE_JSON_DATA_TAG;


    public ProcessFixJsonData(OutputTag<String> errorParseJsonDataTag) {
        this.ERROR_PARSE_JSON_DATA_TAG = errorParseJsonDataTag;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }


    @Override
    public void processElement(String s, ProcessFunction<String, JsonObject>.Context context, Collector<JsonObject> collector) throws Exception {
        try {
            JsonObject jsonObject = new Gson().fromJson(s, JsonObject.class);
            collector.collect(jsonObject);
        }catch (Exception e){
            e.printStackTrace();
            context.output(ERROR_PARSE_JSON_DATA_TAG,s);
        }
    }
}
