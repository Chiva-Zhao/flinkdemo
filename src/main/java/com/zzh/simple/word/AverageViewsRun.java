package com.zzh.simple.word;

import com.zzh.utils.StreamUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-18 11:32
 **/
public class AverageViewsRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        DataStream<String> dataStream = StreamUtils.getDataStream(env, params);
        if (dataStream == null) {
            System.exit(1);
            return;
        }
        dataStream.map(new RowSplitter())
                .keyBy(0)
                .reduce(new SumAndCount())
                .map(new Averager())
                .print();

        env.execute("average view run");
    }

    private static class RowSplitter implements MapFunction<String, Tuple3<String, Double, Integer>> {
        @Override
        public Tuple3<String, Double, Integer> map(String row) throws Exception {
            String[] values = row.split(",");
            if (values.length == 2) {
                return new Tuple3<>(values[0].trim(), Double.parseDouble(values[1].trim()), 1);
            }
            return null;
        }
    }

    private static class SumAndCount implements ReduceFunction<Tuple3<String, Double, Integer>> {
        @Override
        public Tuple3<String, Double, Integer> reduce(Tuple3<String, Double, Integer> accumulate, Tuple3<String, Double, Integer> input) throws Exception {
            return new Tuple3<>(accumulate.f0, accumulate.f1 + input.f1, accumulate.f2 + input.f2);
        }
    }

    private static class Averager implements MapFunction<Tuple3<String, Double, Integer>, Tuple2<String, Double>> {
        @Override
        public Tuple2<String, Double> map(Tuple3<String, Double, Integer> input) throws Exception {
            return new Tuple2<>(input.f0, input.f1 / input.f2);
        }
    }
}
