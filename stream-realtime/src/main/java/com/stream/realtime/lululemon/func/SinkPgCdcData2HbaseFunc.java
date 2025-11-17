package com.stream.realtime.lululemon.func;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.stream.core.HbaseUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 * @Package com.stream.realtime.lululemon.func.SinkPgCdcData2HbaseFunc
 * @Author zhou.han
 * @Date 2025/11/17 10:56
 * @description:
 */
public class SinkPgCdcData2HbaseFunc extends RichSinkFunction<JsonObject>  implements CheckpointedFunction {

    private static final Logger logger = LoggerFactory.getLogger(SinkPgCdcData2HbaseFunc.class);

    private  HbaseUtils hbaseUtils;
    private Connection hbaseConn;
    private String pgHbaseUserInfoTableName = "realtime_v3:dim_user_info_v3";
    private BufferedMutatorParams bufferedMutator;

    BufferedMutator Mutator = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        hbaseUtils = new HbaseUtils("cdh01,cdh02,cdh03");
        hbaseConn = hbaseUtils.getConnection();
        if (!hbaseUtils.tableIsExists(pgHbaseUserInfoTableName)){
            hbaseUtils.createTable("realtime_v3","dim_user_info_v3");
        }

        bufferedMutator = new BufferedMutatorParams(hbaseConn.getTable(TableName.valueOf(pgHbaseUserInfoTableName)).getName()).writeBufferSize(1024);
        Mutator = hbaseConn.getBufferedMutator(bufferedMutator);
    }


    @Override
    public void invoke(JsonObject value, Context context) throws Exception {
        String userIdRowKey = MD5Hash.getMD5AsHex(value.get("user_id").getAsString().getBytes());
        Put put = new Put(Bytes.toBytes(userIdRowKey));
        for (Map.Entry<String, JsonElement> entry : value.entrySet()) {
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(entry.getKey()),Bytes.toBytes(entry.getValue().toString()));
        }

        try {
            Mutator.mutate(put);
        }catch (Exception e){
            e.printStackTrace();
        }

    }


    @Override
    public void close() throws Exception {
        super.close();
        if (hbaseConn != null){
            hbaseConn.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        if (Mutator != null){
            Mutator.close();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
