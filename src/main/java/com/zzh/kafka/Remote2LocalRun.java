package com.zzh.kafka;

import com.zzh.schema.MetricSchema;
import com.zzh.utils.ExecutionEnvUtil;
import com.zzh.utils.KafkaConfigUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import static com.zzh.constant.PropertiesConstants.*;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-13 17:35
 **/
public class Remote2LocalRun {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(tool);
        DataStreamSource source = KafkaConfigUtil.buildSource(env);
        source.addSink(new FlinkKafkaProducer(tool.get(KAFKA_SINK_BROKERS), tool.get(KAFKA_SINK_TOPIC), new MetricSchema()))
                .name("flink-remote-to-local")
                .setParallelism(tool.getInt(STREAM_SINK_PARALLELISM));
        env.execute("flink-remote-to-local-run");
    }
}
