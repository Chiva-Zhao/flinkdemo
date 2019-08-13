package com.zzh.kafka;

import com.alibaba.fastjson.JSON;
import com.zzh.domain.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * bin\window\zookeeper-server-start.bat config\zookeeper.properties
 * bin\window\kafka-server-start.bat config\server.properties
 * bin\windiw\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic student
 */
public class KafkaUtils2 {
    public static final String broker_list = "localhost:9092";
    public static final String topic = "student";  // kafka topic

    public static void writeToKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 1; i <= 100; i++) {
            Student student = new Student(i, "zzh" + i, "password" + i, 18 + i);
            ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(student));
            producer.send(record);
            System.out.println("发送数据: " + JSON.toJSONString(student));
        }
        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        writeToKafka();
    }
}