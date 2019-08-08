package com.zzh.transfer;

import com.zzh.domain.Student;
import com.zzh.mysql.SourceFromMySQL;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-8 9:00
 **/
public class FlatMapTransferRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Student> source = environment.addSource(new SourceFromMySQL())
                .setParallelism(1);
        SingleOutputStreamOperator<Student> flatMap = source.flatMap(new FlatMapFunction<Student, Student>() {
            @Override
            public void flatMap(Student student, Collector<Student> out) throws Exception {
                if (student.getId() % 2 == 0) {
                    out.collect(student);
                }
            }
        });
        flatMap.addSink(new PrintSinkFunction());
        environment.execute("mapTransfer");
    }
}
