package com.zzh.simple;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-21 13:58
 * name,salary  | name,department        name,  salary, department
 * Tom,1        | Tim, google           Tom,    1,      googlex
 * John,1       | Tom, googlex    ===>  John,   1,      alphabet
 * Tim,150      | John, alphabet        Tim,    1,      google
 **/
public class JoinStreamRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        SingleOutputStreamOperator<Tuple2<String, String>> salaryData = env.socketTextStream("localhost", 8000).map(new RowSpliter());
        SingleOutputStreamOperator<Tuple2<String, String>> deptData = env.socketTextStream("localhost", 9000).map(new RowSpliter());

        salaryData.join(deptData)
                .where(new JoinKeySelector())
                .equalTo(new JoinKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new MergeJoinFuncion())
                .print();
        env.execute("join_stream_run");
    }

    private static class RowSpliter implements MapFunction<String, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> map(String value) throws Exception {
            String[] arr = value.split(",");
            return Tuple2.of(arr[0], arr[1]);
        }
    }

    private static class JoinKeySelector implements KeySelector<Tuple2<String, String>, String> {
        @Override
        public String getKey(Tuple2<String, String> value) throws Exception {
            return value.f0;
        }
    }

    private static class MergeJoinFuncion implements JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple3<String, String, String>> {
        @Override
        public Tuple3<String, String, String> join(Tuple2<String, String> first, Tuple2<String, String> second) throws Exception {
            return Tuple3.of(first.f0, first.f1, second.f1);
        }
    }
}
