package com.zzh.mysql;

import com.alibaba.fastjson.JSON;
import com.zzh.domain.Student;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;

import java.util.Properties;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-7 17:29
 **/
public class KafkaToMySqlSinkRun {
    public static final String topic = "student";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest"); //value 反序列化
        SingleOutputStreamOperator<Student> student = environment.addSource(new FlinkKafkaConsumer<String>(topic, new KafkaDeserializationSchemaWrapper(new SimpleStringSchema()), props))
                .setParallelism(1)
                .map(stu -> JSON.parseObject(stu, Student.class));
        student.addSink(new SinkToMysql());
        environment.execute("sinkToMysql");
    }
}
