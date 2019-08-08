package com.zzh.transfer;

import com.zzh.domain.Student;
import com.zzh.mysql.SourceFromMySQL;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-8 14:55
 **/
public class KeyByTransferRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<Student, Integer> keyBy = environment.addSource(new SourceFromMySQL())
                .setParallelism(1)
                .keyBy(Student::getAge);
        keyBy.print();
        environment.execute("keyByRun");
    }
}
