package com.zzh.simple;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-21 11:04
 **/
public class UnionStreamRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStreamSource<String> ds1 = env.socketTextStream("localhost", 8000);
        DataStreamSource<String> ds2 = env.socketTextStream("localhost", 9000);
        if (ds1 == null || ds2 == null) {
            System.exit(1);
            return;
        }
        DataStream<String> ds3 = ds1.union(ds2);
        ds3.print();
        env.execute("union stream run");
    }
}
