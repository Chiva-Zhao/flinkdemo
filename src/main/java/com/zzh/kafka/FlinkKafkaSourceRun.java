package com.zzh.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;

import java.util.Properties;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-6 19:44
 **/
public class FlinkKafkaSourceRun {
    public static final String topic = "students";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest"); //value 反序列化

        DataStreamSource dataStreamSource = env.addSource(new FlinkKafkaConsumer<>(topic, new KafkaDeserializationSchemaWrapper(new SimpleStringSchema()), props)).setParallelism(1);
        dataStreamSource.print();
        env.execute("Flink add data source");
    }
}
