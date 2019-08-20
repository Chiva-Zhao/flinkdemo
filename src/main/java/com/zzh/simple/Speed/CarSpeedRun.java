package com.zzh.simple.Speed;

import com.zzh.utils.StreamUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-20 14:13
 **/
public class CarSpeedRun {
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
                .flatMap(new AverateSpeedValueState())
                .print();

        env.execute("carSpeed run");
    }

    private static class Speed implements MapFunction<String, Tuple2<Integer, Double>> {
        @Override
        public Tuple2<Integer, Double> map(String speed) throws Exception {
            return Tuple2.of(1, Double.parseDouble(speed));
        }
    }

    private static class AverateSpeedValueState extends RichFlatMapFunction<Tuple2<Integer, Double>, String> {
        private transient ValueState<Tuple2<Integer, Double>> countSumState;

        @Override

        public void flatMap(Tuple2<Integer, Double> input, Collector<String> out) throws Exception {
            Tuple2<Integer, Double> currentCountSum = countSumState.value();
            if (input.f1 >= 65) {
                out.collect(String.format("警告!您已超速，过去%s俩车的平均速度为%s，您的速度为%s", currentCountSum.f0, currentCountSum.f1 / currentCountSum.f0, input.f1));
                countSumState.clear();
                currentCountSum = countSumState.value();
            } else {
                out.collect("感谢您的速度维持在限速65之内");
            }
            currentCountSum.f0 += 1;
            currentCountSum.f1 += input.f1;
            countSumState.update(currentCountSum);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Tuple2<Integer, Double>> descriptor
                    = new ValueStateDescriptor<Tuple2<Integer, Double>>("carAverageSpeed", TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {
            }), Tuple2.of(0, 0.0));
            countSumState = getRuntimeContext().getState(descriptor);
        }
    }
}
