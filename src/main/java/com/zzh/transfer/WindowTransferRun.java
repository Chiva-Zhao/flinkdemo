package com.zzh.transfer;

import com.zzh.mysql.SourceFromMySQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-8 16:49
 **/
public class WindowTransferRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new SourceFromMySQL()).setParallelism(1)
                .keyBy("age")
                //.window(TumblingEventTimeWindows.of(Time.seconds(10)));
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(20)));
        env.execute("windowRun");
    }
}
