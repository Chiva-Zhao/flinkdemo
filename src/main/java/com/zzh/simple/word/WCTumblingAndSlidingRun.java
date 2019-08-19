package com.zzh.simple.word;

import com.zzh.transfer.WordCountSpliter;
import com.zzh.utils.StreamUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-19 11:49
 **/
public class WCTumblingAndSlidingRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        DataStream<String> dataStream = StreamUtils.getDataStream(env, params);
        if (dataStream == null) {
            System.exit(1);
            return;
        }
        dataStream.flatMap(new WordCountSpliter())
                .keyBy(0)
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .sum(1)
                .print();
        env.execute("Word count tumbling and sliding window run");
    }
}
