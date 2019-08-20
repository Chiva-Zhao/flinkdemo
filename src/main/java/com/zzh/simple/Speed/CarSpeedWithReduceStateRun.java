package com.zzh.simple.Speed;

import com.zzh.utils.StreamUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-20 16:51
 **/
public class CarSpeedWithReduceStateRun {
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
                .flatMap(new AverageSpeedReduceState())
                .print();
        env.execute("ReduceState demo Run");
    }

    private static class Speed implements MapFunction<String, Tuple2<Integer, Double>> {
        @Override
        public Tuple2<Integer, Double> map(String speed) throws Exception {
            return Tuple2.of(1, Double.parseDouble(speed));
        }
    }

    private static class AverageSpeedReduceState extends RichFlatMapFunction<Tuple2<Integer, Double>, String> {
        private transient ReducingState<Double> sumState;
        private transient ValueState<Integer> countState;

        @Override
        public void flatMap(Tuple2<Integer, Double> input, Collector<String> out) throws Exception {
            if (input.f1 >= 65) {
                out.collect(String.format("警告!您已超速，过去%s俩车的平均速度为%s，您的速度为%s", countState.value(), sumState.get() / countState.value(), input.f1));
                sumState.clear();
                countState.clear();
            } else {
                out.collect("感谢您的速度维持在限速65之内");
            }
            sumState.add(input.f1);
            countState.update(countState.value() + 1);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("sumState", Integer.class, 0);
            ReducingStateDescriptor<Double> reducingStateDescriptor = new ReducingStateDescriptor<>("countState", (ReduceFunction<Double>) Double::sum, Double.class);
            sumState = getRuntimeContext().getReducingState(reducingStateDescriptor);
            countState = getRuntimeContext().getState(valueStateDescriptor);
        }
    }
}
