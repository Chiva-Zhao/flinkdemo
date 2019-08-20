package com.zzh.simple.Speed;

import com.zzh.utils.StreamUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-20 16:28
 **/
public class CarSpeedWithListStateRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataSource = StreamUtils.getDataStream(env, params);
        if (dataSource == null) {
            System.exit(1);
            return;
        }

        dataSource.map(new Speed())
                .keyBy(0)
                .flatMap(new AverageSpeedListState())
                .print();

        env.execute("carSpeedWithListStateRun");
    }

    private static class Speed implements MapFunction<String, Tuple2<Integer, Double>> {
        @Override
        public Tuple2<Integer, Double> map(String speed) throws Exception {
            return Tuple2.of(1, Double.parseDouble(speed));
        }
    }

    private static class AverageSpeedListState extends RichFlatMapFunction<Tuple2<Integer, Double>, String> {
        private transient ListState<Double> speedListState;

        @Override
        public void flatMap(Tuple2<Integer, Double> input, Collector<String> out) throws Exception {
            if (input.f1 >= 65) {
                Iterable<Double> carSpeeds = speedListState.get();
                int count = 0;
                double sum = 0.0;
                for (Double speed : carSpeeds) {
                    count++;
                    sum += speed;
                }
                out.collect(String.format("警告!您已超速，过去%s俩车的平均速度为%s，您的速度为%s", count, sum / count, input.f1));
                speedListState.clear();
            } else {
                out.collect("感谢您的速度维持在限速65之内");
            }
            speedListState.add(input.f1);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<Double> descriptor = new ListStateDescriptor<Double>("SpeedList", Double.class);
            speedListState = getRuntimeContext().getListState(descriptor);
        }
    }
}
