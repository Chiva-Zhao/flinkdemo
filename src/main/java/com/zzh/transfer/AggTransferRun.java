package com.zzh.transfer;

import com.zzh.domain.Student;
import com.zzh.mysql.SourceFromMySQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-8 15:24
 **/
public class AggTransferRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.addSource(new SourceFromMySQL()).setParallelism(1)
                .keyBy(Student::getAge)
                //.sum("age")
                //.max("age")
                //.min("age");
                //.minBy("age")
                .maxBy("age")
                .print();
        environment.execute("aggRun");
    }
}
