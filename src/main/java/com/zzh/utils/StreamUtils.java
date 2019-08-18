package com.zzh.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-18 10:57
 **/
public class StreamUtils {
    public static DataStream<String> getDataStream(StreamExecutionEnvironment env, ParameterTool params) {
        DataStream<String> dataStream = null;
        if (params.has("input")) {
            dataStream = env.readTextFile(params.get("input"));
        } else if (params.has("host") && params.has("port")) {
            dataStream = env.socketTextStream(params.get("host"), Integer.parseInt(params.get("port")));
        } else {
            System.out.println("please using --input filename");
            System.out.println("or using --host hostname --port port command line");
        }
        return dataStream;
    }
}
