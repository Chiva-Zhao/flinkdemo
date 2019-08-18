package com.zzh.simple.word;

import com.zzh.transfer.WordSpliter;
import com.zzh.utils.StreamUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-18 10:41
 **/
public class WordSplitRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);
        DataStream<String> dataStream = StreamUtils.getDataStream(env, params);
        if (dataStream == null) {
            System.exit(1);
            return;
        }
        dataStream.flatMap(new WordSpliter())
                .print();
        env.execute("word split run");
    }

}
