package com.zzh.transfer;

import com.zzh.domain.Student;
import com.zzh.mysql.SourceFromMySQL;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-8 9:00
 **/
public class MapTransferRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Student> source = environment.addSource(new SourceFromMySQL())
                .setParallelism(1);
        SingleOutputStreamOperator<Student> newSource = source.map((st -> {
            Student st1 = st;
            st1.setAge(st.getAge() + 5);
            return st1;
        }));
        newSource.addSink(new PrintSinkFunction());
        environment.execute("mapTransfer");
    }
}
