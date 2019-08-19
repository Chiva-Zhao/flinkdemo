package com.zzh.simple.word;

import com.zzh.domain.WordCount;
import com.zzh.utils.StreamUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-19 14:49
 **/
public class WCCountwindowRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        DataStream<String> dataSource = StreamUtils.getDataStream(env, params);
        if (dataSource == null) {
            System.exit(1);
            return;
        }
        dataSource.flatMap(new WorkCountMapper())
                .keyBy("word")
                .countWindow(3)
                .sum("count")
                .print();
        env.execute("CountWindow Run");
    }

    private static class WorkCountMapper implements FlatMapFunction<String, WordCount> {
        @Override
        public void flatMap(String sentenec, Collector<WordCount> collector) throws Exception {
            for (String word : sentenec.split("\\W+")) {
                collector.collect(new WordCount(word, 1));
            }
        }
    }
}
