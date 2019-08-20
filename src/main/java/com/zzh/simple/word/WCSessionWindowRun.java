package com.zzh.simple.word;

import com.zzh.utils.StreamUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-19 17:29
 * dataFormat: userid, pageid, spend time
 * jane,home,4
 * jane,home,5
 * chiva,index,7
 **/
public class WCSessionWindowRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        DataStream<String> dataSource = StreamUtils.getDataStream(env, params);
        if (dataSource == null) {
            System.exit(1);
            return;
        }
        dataSource.map(new RowSpliter())
                .keyBy(0, 1)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .max(2)
                .print();
        env.execute("session_window_run");
    }

    private static class RowSpliter implements MapFunction<String, Tuple3<String, String, Double>> {
        @Override
        public Tuple3<String, String, Double> map(String row) throws Exception {
            String[] fields = row.split(",");
            if (fields.length == 3) {
                return new Tuple3<>(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
            return null;
        }
    }
}
