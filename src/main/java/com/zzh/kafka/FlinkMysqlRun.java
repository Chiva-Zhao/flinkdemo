package com.zzh.kafka;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-7 9:29
 **/
public class FlinkMysqlRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource source = env.addSource(new SourceFromMySQL()).setParallelism(1);
        //source.print();
        source.addSink(new PrintSinkFunction());
        env.execute("mysqlStream");
    }
}
